import logging
from datetime import datetime
from typing import TYPE_CHECKING, Any, Iterable, Optional, Union
from uuid import UUID

import anyio
from opentelemetry import trace

import prefect
from prefect._internal.compatibility.async_dispatch import async_dispatch
from prefect._result_records import ResultRecordMetadata
from prefect.client.orchestration import get_client
from prefect.client.schemas import FlowRun, TaskRun, TaskRunResult
from prefect.client.schemas.actions import LogCreate
from prefect.client.schemas.objects import State, StateType
from prefect.client.utilities import get_or_create_client
from prefect.context import FlowRunContext, TaskRunContext
from prefect.logging import get_logger
from prefect.states import Completed, Failed, Pending, Scheduled
from prefect.tasks import Task
from prefect.telemetry.run_telemetry import LABELS_TRACEPARENT_KEY, RunTelemetry
from prefect.types._datetime import now
from prefect.utilities._engine import dynamic_key_for_task_run
from prefect.utilities.engine import collect_task_run_inputs_sync
from prefect.utilities.slugify import slugify
from prefect.utilities.urls import url_for


def _is_instrumentation_enabled() -> bool:
    try:
        from opentelemetry.instrumentation.utils import is_instrumentation_enabled

        return is_instrumentation_enabled()
    except (ImportError, ModuleNotFoundError):
        return False


if TYPE_CHECKING:
    from prefect.client.orchestration import PrefectClient, SyncPrefectClient
    from prefect.client.schemas.objects import FlowRun

prefect.client.schemas.StateCreate.model_rebuild(
    _types_namespace={
        "ResultRecordMetadata": ResultRecordMetadata,
    }
)


if TYPE_CHECKING:
    import logging

logger: "logging.Logger" = get_logger(__name__)


_TERMINAL_FAILURE_STATES = frozenset({StateType.FAILED, StateType.CRASHED})


def _dedup_orphan_state(
    flow_run: "FlowRun",
    idempotency_key: Optional[str],
) -> Optional["State"]:
    """
    Build the state used to label a placeholder task run that was orphaned by
    an idempotency dedup, or None when the duplicate is not in a labelable
    terminal state.

    When `create_flow_run_from_deployment` hits the server's
    `(flow_id, idempotency_key)` uniqueness constraint, the returned flow run's
    `parent_task_run_id` points at the *original* call's placeholder. The
    placeholder created by *this* call is never updated by the
    `UpdateSubflowParentTask` orchestration policy and would otherwise remain
    `Pending` forever. The built state labels the orphaned placeholder with
    the terminal state of the duplicate, records the duplicate as its child
    in state details, and includes the duplicate's UI URL when configured.
    Non-terminal duplicates are left as-is; no child run is ever modified.
    """
    duplicate_state = flow_run.state
    if duplicate_state is None:
        return None

    if duplicate_state.type in _TERMINAL_FAILURE_STATES:
        mirrored = Failed(
            message=(
                f"Duplicate run resolved by idempotency key {idempotency_key!r}: "
                f"flow run {flow_run.name} ({flow_run.id}) is "
                f"{duplicate_state.type.value}."
            ),
        )
    elif duplicate_state.type is StateType.COMPLETED:
        mirrored = Completed(
            message=(
                f"Duplicate run resolved by idempotency key {idempotency_key!r}: "
                f"flow run {flow_run.name} ({flow_run.id}) completed."
            ),
        )
    else:
        # Not terminal (Scheduled/Running/etc.): leave the placeholder alone.
        return None

    # Record the duplicate as the placeholder's child in state details, the
    # same field `UpdateSubflowParentTask` writes for a non-deduplicated
    # subflow, and include its UI URL when configured.
    mirrored.state_details.child_flow_run_id = flow_run.id
    child_url = url_for("flow-run", obj_id=flow_run.id)
    if child_url:
        mirrored.message += f" See {child_url}."

    return mirrored


def _dedup_orphan_placeholder_name(flow_run: "FlowRun") -> str:
    """
    Name for a placeholder task run that was orphaned by an idempotency dedup:
    it identifies the flow run the dispatch was deduplicated onto.
    """
    return f"Idempotent dedupe: {flow_run.name}"


def _dedup_orphan_log(
    flow_run: "FlowRun",
    idempotency_key: Optional[str],
) -> "LogCreate":
    """
    Build the log record attached to an orphaned placeholder task run so its
    Logs tab explains the dedup (the run is never executed, so no engine
    would otherwise log anything for it).
    """
    state_type = flow_run.state.type if flow_run.state else None
    base = (
        "Idempotent dedupe: this dispatch resolved to existing flow run "
        f"{flow_run.name!r} ({flow_run.id})"
    )
    child_url = url_for("flow-run", obj_id=flow_run.id)

    if child_url:
        if state_type in _TERMINAL_FAILURE_STATES:
            level = logging.WARNING
            message = (
                f"{base}, which is {state_type.value}. This placeholder task "
                f"run mirrors that outcome. See {child_url}. Idempotency "
                f"key: {idempotency_key!r}."
            )
        elif state_type is StateType.COMPLETED:
            level = logging.INFO
            message = (
                f"{base}, which completed. This placeholder task run mirrors "
                f"that outcome. See {child_url}. Idempotency key: "
                f"{idempotency_key!r}."
            )
        else:
            level = logging.INFO
            message = (
                f"{base}, currently in state "
                f"{state_type.value if state_type else 'unknown'}. Its "
                f"terminal outcome will be mirrored here. See {child_url}. "
                f"Idempotency key: {idempotency_key!r}."
            )
    else:
        level = (
            logging.WARNING if state_type in _TERMINAL_FAILURE_STATES else logging.INFO
        )
        message = f"{base}. Idempotency key: {idempotency_key!r}."

    return LogCreate(
        name="prefect.flow_runs",
        level=level,
        message=message,
        timestamp=now("UTC"),
    )


async def _rename_dedup_placeholder(
    client: "PrefectClient",
    parent_task_run: "TaskRun",
    flow_run: "FlowRun",
) -> None:
    """
    Async twin of `_rename_dedup_placeholder_sync`. Best effort: failures are
    logged and the placeholder keeps its generated name.
    """
    try:
        await client.set_task_run_name(
            parent_task_run.id, _dedup_orphan_placeholder_name(flow_run)
        )
    except Exception:
        logger.warning(
            "Failed to rename deduplicated placeholder task run %s",
            parent_task_run.id,
            exc_info=True,
        )


def _rename_dedup_placeholder_sync(
    client: "SyncPrefectClient",
    parent_task_run: "TaskRun",
    flow_run: "FlowRun",
) -> None:
    """
    Sync twin of `_rename_dedup_placeholder`. Best effort: failures are logged
    and the placeholder keeps its generated name.
    """
    try:
        client.set_task_run_name(
            parent_task_run.id, _dedup_orphan_placeholder_name(flow_run)
        )
    except Exception:
        logger.warning(
            "Failed to rename deduplicated placeholder task run %s",
            parent_task_run.id,
            exc_info=True,
        )


async def _log_dedup_orphan(
    client: "PrefectClient",
    parent_task_run: "TaskRun",
    flow_run: "FlowRun",
    idempotency_key: Optional[str],
) -> None:
    """
    Async twin of `_log_dedup_orphan_sync`. Best effort: failures are logged
    and the placeholder's Logs tab simply stays empty.
    """
    try:
        record = _dedup_orphan_log(flow_run, idempotency_key)
        record.flow_run_id = parent_task_run.flow_run_id
        record.task_run_id = parent_task_run.id
        await client.create_logs([record])
    except Exception:
        logger.warning(
            "Failed to create dedup log for placeholder task run %s",
            parent_task_run.id,
            exc_info=True,
        )


def _log_dedup_orphan_sync(
    client: "SyncPrefectClient",
    parent_task_run: "TaskRun",
    flow_run: "FlowRun",
    idempotency_key: Optional[str],
) -> None:
    """
    Sync twin of `_log_dedup_orphan`. Best effort: failures are logged and
    the placeholder's Logs tab simply stays empty.
    """
    try:
        record = _dedup_orphan_log(flow_run, idempotency_key)
        record.flow_run_id = parent_task_run.flow_run_id
        record.task_run_id = parent_task_run.id
        client.create_logs([record])
    except Exception:
        logger.warning(
            "Failed to create dedup log for placeholder task run %s",
            parent_task_run.id,
            exc_info=True,
        )


async def _mirror_dedup_placeholder_state(
    client: "PrefectClient",
    parent_task_run: "TaskRun",
    flow_run: "FlowRun",
    idempotency_key: Optional[str],
) -> bool:
    """
    Async twin of `_mirror_dedup_placeholder_state_sync`. Returns True if a
    state was written to the placeholder.
    """
    mirrored = _dedup_orphan_state(flow_run, idempotency_key)
    if mirrored is None:
        return False
    try:
        await client.set_task_run_state(parent_task_run.id, mirrored, force=True)
    except Exception:
        logger.warning(
            "Failed to mirror deduplicated subflow state onto placeholder task "
            "run %s for flow run %s",
            parent_task_run.id,
            flow_run.id,
            exc_info=True,
        )
        return False
    return True


def _mirror_dedup_placeholder_state_sync(
    client: "SyncPrefectClient",
    parent_task_run: "TaskRun",
    flow_run: "FlowRun",
    idempotency_key: Optional[str],
) -> bool:
    """
    Sync twin of `_mirror_dedup_placeholder_state`. Returns True if a state
    was written to the placeholder.
    """
    mirrored = _dedup_orphan_state(flow_run, idempotency_key)
    if mirrored is None:
        return False
    try:
        client.set_task_run_state(parent_task_run.id, mirrored, force=True)
    except Exception:
        logger.warning(
            "Failed to mirror deduplicated subflow state onto placeholder task "
            "run %s for flow run %s",
            parent_task_run.id,
            flow_run.id,
            exc_info=True,
        )
        return False
    return True


async def arun_deployment(
    name: Union[str, UUID],
    client: Optional["PrefectClient"] = None,
    parameters: Optional[dict[str, Any]] = None,
    scheduled_time: Optional[datetime] = None,
    flow_run_name: Optional[str] = None,
    timeout: Optional[float] = None,
    poll_interval: Optional[float] = 5,
    tags: Optional[Iterable[str]] = None,
    idempotency_key: Optional[str] = None,
    work_queue_name: Optional[str] = None,
    as_subflow: Optional[bool] = True,
    job_variables: Optional[dict[str, Any]] = None,
) -> "FlowRun":
    """
    Asynchronously create a flow run for a deployment and return it after completion or a timeout.

    By default, this function blocks until the flow run finishes executing.
    Specify a timeout (in seconds) to wait for the flow run to execute before
    returning flow run metadata. To return immediately, without waiting for the
    flow run to execute, set `timeout=0`.

    Note that if you specify a timeout, this function will return the flow run
    metadata whether or not the flow run finished executing.

    If called within a flow or task, the flow run this function creates will
    be linked to the current flow run as a subflow. Disable this behavior by
    passing `as_subflow=False`.

    Args:
        name: The deployment id or deployment name in the form:
            `"flow name/deployment name"`
        client: An optional PrefectClient to use for API requests.
        parameters: Parameter overrides for this flow run. Merged with the deployment
            defaults.
        scheduled_time: The time to schedule the flow run for, defaults to scheduling
            the flow run to start now.
        flow_run_name: A name for the created flow run
        timeout: The amount of time to wait (in seconds) for the flow run to
            complete before returning. Setting `timeout` to 0 will return the flow
            run metadata immediately. Setting `timeout` to None will allow this
            function to poll indefinitely. Defaults to None.
        poll_interval: The number of seconds between polls
        tags: A list of tags to associate with this flow run; tags can be used in
            automations and for organizational purposes.
        idempotency_key: A unique value to recognize retries of the same run, and
            prevent creating multiple flow runs.
        work_queue_name: The name of a work queue to use for this run. Defaults to
            the default work queue for the deployment.
        as_subflow: Whether to link the flow run as a subflow of the current
            flow or task run.
        job_variables: A dictionary of dot delimited infrastructure overrides that
            will be applied at runtime; for example `env.CONFIG_KEY=config_value` or
            `namespace='prefect'`

    Example:
        ```python
        import asyncio
        from prefect.deployments import arun_deployment

        async def main():
            flow_run = await arun_deployment("my-flow/my-deployment")
            print(flow_run.state)

        asyncio.run(main())
        ```
    """
    if timeout is not None and timeout < 0:
        raise ValueError("`timeout` cannot be negative")

    if scheduled_time is None:
        scheduled_time = now("UTC")

    parameters = parameters or {}

    deployment_id = None

    if isinstance(name, UUID):
        deployment_id = name
    else:
        try:
            deployment_id = UUID(name)
        except ValueError:
            pass

    client, _ = get_or_create_client(client)

    if deployment_id:
        deployment = await client.read_deployment(deployment_id=deployment_id)
    else:
        deployment = await client.read_deployment_by_name(name)

    flow_run_ctx = FlowRunContext.get()
    task_run_ctx = TaskRunContext.get()
    if as_subflow and (flow_run_ctx or task_run_ctx):
        # TODO: this logic can likely be simplified by using `Task.create_run`
        from prefect.utilities._engine import dynamic_key_for_task_run
        from prefect.utilities.engine import collect_task_run_inputs

        # This was called from a flow. Link the flow run as a subflow.
        task_inputs = {
            k: await collect_task_run_inputs(v) for k, v in parameters.items()
        }

        # Track parent task if this is being called from within a task
        # This enables the execution graph to properly display the deployment
        # flow run as nested under the calling task
        if task_run_ctx:
            # The task run is only considered a parent if it is in the same
            # flow run (otherwise the child is in a subflow, so the subflow
            # serves as the parent) or if there is no flow run
            if not flow_run_ctx or (
                task_run_ctx.task_run.flow_run_id
                == getattr(flow_run_ctx.flow_run, "id", None)
            ):
                task_inputs["__parents__"] = [
                    TaskRunResult(id=task_run_ctx.task_run.id)
                ]

        if deployment_id:
            flow = await client.read_flow(deployment.flow_id)
            deployment_name = f"{flow.name}/{deployment.name}"
        else:
            deployment_name = name

        # Generate a task in the parent flow run to represent the result of the subflow
        dummy_task = Task(
            name=deployment_name,
            fn=lambda: None,
            version=deployment.version,
        )
        # Override the default task key to include the deployment name
        dummy_task.task_key = f"{__name__}.run_deployment.{slugify(deployment_name)}"
        flow_run_id = (
            flow_run_ctx.flow_run.id
            if flow_run_ctx
            else task_run_ctx.task_run.flow_run_id
        )
        dynamic_key = (
            dynamic_key_for_task_run(flow_run_ctx, dummy_task)
            if flow_run_ctx
            else task_run_ctx.task_run.dynamic_key
        )
        parent_task_run = await client.create_task_run(
            task=dummy_task,
            flow_run_id=flow_run_id,
            dynamic_key=dynamic_key,
            task_inputs=task_inputs,
            state=Pending(),
        )
        parent_task_run_id = parent_task_run.id
    else:
        parent_task_run_id = None

    if flow_run_ctx and flow_run_ctx.flow_run:
        traceparent = flow_run_ctx.flow_run.labels.get(LABELS_TRACEPARENT_KEY)
    elif _is_instrumentation_enabled():
        traceparent = RunTelemetry.traceparent_from_span(span=trace.get_current_span())
    else:
        traceparent = None

    trace_labels = {LABELS_TRACEPARENT_KEY: traceparent} if traceparent else {}

    flow_run = await client.create_flow_run_from_deployment(
        deployment.id,
        parameters=parameters,
        state=Scheduled(scheduled_time=scheduled_time),
        name=flow_run_name,
        tags=tags,
        idempotency_key=idempotency_key,
        parent_task_run_id=parent_task_run_id,
        work_queue_name=work_queue_name,
        job_variables=job_variables,
        labels=trace_labels,
    )

    flow_run_id = flow_run.id

    is_dedup = (
        parent_task_run_id is not None
        and flow_run.parent_task_run_id != parent_task_run_id
    )
    if is_dedup:
        # The server deduplicated on (flow_id, idempotency_key): the returned
        # run is attached to a different placeholder (the original call's), so
        # the one created above will never be updated by the subflow
        # state-mirroring policy. Rename it after the run it deduplicated
        # onto and label it with the duplicate's final state instead of
        # leaving it Pending forever.
        await _rename_dedup_placeholder(client, parent_task_run, flow_run)
        await _log_dedup_orphan(client, parent_task_run, flow_run, idempotency_key)
        mirrored = await _mirror_dedup_placeholder_state(
            client, parent_task_run, flow_run, idempotency_key
        )
    else:
        mirrored = False

    if timeout == 0:
        return flow_run

    with anyio.move_on_after(timeout):
        while True:
            flow_run = await client.read_flow_run(flow_run_id)
            flow_state = flow_run.state
            if flow_state and flow_state.is_final():
                if is_dedup and not mirrored:
                    mirrored = await _mirror_dedup_placeholder_state(
                        client, parent_task_run, flow_run, idempotency_key
                    )
                return flow_run
            await anyio.sleep(poll_interval)

    return flow_run


@async_dispatch(arun_deployment)
def run_deployment(
    name: Union[str, UUID],
    client: Optional["PrefectClient"] = None,
    parameters: Optional[dict[str, Any]] = None,
    scheduled_time: Optional[datetime] = None,
    flow_run_name: Optional[str] = None,
    timeout: Optional[float] = None,
    poll_interval: Optional[float] = 5,
    tags: Optional[Iterable[str]] = None,
    idempotency_key: Optional[str] = None,
    work_queue_name: Optional[str] = None,
    as_subflow: Optional[bool] = True,
    job_variables: Optional[dict[str, Any]] = None,
) -> "FlowRun":
    """
    Create a flow run for a deployment and return it after completion or a timeout.

    This function will dispatch to `arun_deployment` when called from an async context.

    By default, this function blocks until the flow run finishes executing.
    Specify a timeout (in seconds) to wait for the flow run to execute before
    returning flow run metadata. To return immediately, without waiting for the
    flow run to execute, set `timeout=0`.

    Note that if you specify a timeout, this function will return the flow run
    metadata whether or not the flow run finished executing.

    If called within a flow or task, the flow run this function creates will
    be linked to the current flow run as a subflow. Disable this behavior by
    passing `as_subflow=False`.

    Args:
        name: The deployment id or deployment name in the form:
            `"flow name/deployment name"`
        client: An optional PrefectClient to use for API requests. This is ignored
            when called from a synchronous context.
        parameters: Parameter overrides for this flow run. Merged with the deployment
            defaults.
        scheduled_time: The time to schedule the flow run for, defaults to scheduling
            the flow run to start now.
        flow_run_name: A name for the created flow run
        timeout: The amount of time to wait (in seconds) for the flow run to
            complete before returning. Setting `timeout` to 0 will return the flow
            run metadata immediately. Setting `timeout` to None will allow this
            function to poll indefinitely. Defaults to None.
        poll_interval: The number of seconds between polls
        tags: A list of tags to associate with this flow run; tags can be used in
            automations and for organizational purposes.
        idempotency_key: A unique value to recognize retries of the same run, and
            prevent creating multiple flow runs.
        work_queue_name: The name of a work queue to use for this run. Defaults to
            the default work queue for the deployment.
        as_subflow: Whether to link the flow run as a subflow of the current
            flow or task run.
        job_variables: A dictionary of dot delimited infrastructure overrides that
            will be applied at runtime; for example `env.CONFIG_KEY=config_value` or
            `namespace='prefect'`

    Example:
        ```python
        from prefect.deployments import run_deployment

        # Sync context
        flow_run = run_deployment("my-flow/my-deployment")
        print(flow_run.state)

        # Async context (will dispatch to arun_deployment)
        async def main():
            flow_run = await run_deployment("my-flow/my-deployment")
            print(flow_run.state)
        ```
    """
    if timeout is not None and timeout < 0:
        raise ValueError("`timeout` cannot be negative")

    if scheduled_time is None:
        scheduled_time = now("UTC")

    parameters = parameters or {}

    deployment_id = None

    if isinstance(name, UUID):
        deployment_id = name
    else:
        try:
            deployment_id = UUID(name)
        except ValueError:
            pass

    with get_client(sync_client=True) as sync_client:
        if deployment_id:
            deployment = sync_client.read_deployment(deployment_id=deployment_id)
        else:
            deployment = sync_client.read_deployment_by_name(name)

        flow_run_ctx = FlowRunContext.get()
        task_run_ctx = TaskRunContext.get()
        if as_subflow and (flow_run_ctx or task_run_ctx):
            # TODO: this logic can likely be simplified by using `Task.create_run`

            # This was called from a flow. Link the flow run as a subflow.
            task_inputs = {
                k: collect_task_run_inputs_sync(v) for k, v in parameters.items()
            }

            # Track parent task if this is being called from within a task
            # This enables the execution graph to properly display the deployment
            # flow run as nested under the calling task
            if task_run_ctx:
                # The task run is only considered a parent if it is in the same
                # flow run (otherwise the child is in a subflow, so the subflow
                # serves as the parent) or if there is no flow run
                if not flow_run_ctx or (
                    task_run_ctx.task_run.flow_run_id
                    == getattr(flow_run_ctx.flow_run, "id", None)
                ):
                    task_inputs["__parents__"] = [
                        TaskRunResult(id=task_run_ctx.task_run.id)
                    ]

            if deployment_id:
                flow = sync_client.read_flow(deployment.flow_id)
                deployment_name = f"{flow.name}/{deployment.name}"
            else:
                deployment_name = name

            # Generate a task in the parent flow run to represent the result of the subflow
            dummy_task = Task(
                name=deployment_name,
                fn=lambda: None,
                version=deployment.version,
            )
            # Override the default task key to include the deployment name
            dummy_task.task_key = (
                f"{__name__}.run_deployment.{slugify(deployment_name)}"
            )
            flow_run_id = (
                flow_run_ctx.flow_run.id
                if flow_run_ctx
                else task_run_ctx.task_run.flow_run_id
            )
            dynamic_key = (
                dynamic_key_for_task_run(flow_run_ctx, dummy_task)
                if flow_run_ctx
                else task_run_ctx.task_run.dynamic_key
            )
            parent_task_run = sync_client.create_task_run(
                task=dummy_task,
                flow_run_id=flow_run_id,
                dynamic_key=dynamic_key,
                task_inputs=task_inputs,
                state=Pending(),
            )
            parent_task_run_id = parent_task_run.id
        else:
            parent_task_run_id = None

        if flow_run_ctx and flow_run_ctx.flow_run:
            traceparent = flow_run_ctx.flow_run.labels.get(LABELS_TRACEPARENT_KEY)
        elif _is_instrumentation_enabled():
            traceparent = RunTelemetry.traceparent_from_span(
                span=trace.get_current_span()
            )
        else:
            traceparent = None

        trace_labels = {LABELS_TRACEPARENT_KEY: traceparent} if traceparent else {}

        flow_run = sync_client.create_flow_run_from_deployment(
            deployment.id,
            parameters=parameters,
            state=Scheduled(scheduled_time=scheduled_time),
            name=flow_run_name,
            tags=tags,
            idempotency_key=idempotency_key,
            parent_task_run_id=parent_task_run_id,
            work_queue_name=work_queue_name,
            job_variables=job_variables,
            labels=trace_labels,
        )

        flow_run_id = flow_run.id

        is_dedup = (
            parent_task_run_id is not None
            and flow_run.parent_task_run_id != parent_task_run_id
        )
        if is_dedup:
            _rename_dedup_placeholder_sync(sync_client, parent_task_run, flow_run)
            _log_dedup_orphan_sync(
                sync_client, parent_task_run, flow_run, idempotency_key
            )
            mirrored = _mirror_dedup_placeholder_state_sync(
                sync_client, parent_task_run, flow_run, idempotency_key
            )
        else:
            mirrored = False

        if timeout == 0:
            return flow_run

        import time

        start_time = time.monotonic()
        while True:
            flow_run = sync_client.read_flow_run(flow_run_id)
            flow_state = flow_run.state
            if flow_state and flow_state.is_final():
                if is_dedup and not mirrored:
                    mirrored = _mirror_dedup_placeholder_state_sync(
                        sync_client, parent_task_run, flow_run, idempotency_key
                    )
                return flow_run
            if timeout is not None and (time.monotonic() - start_time) >= timeout:
                return flow_run
            time.sleep(poll_interval)

    return flow_run
