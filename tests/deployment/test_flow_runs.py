import inspect
import re
from datetime import timedelta
from typing import TYPE_CHECKING
from unittest import mock
from uuid import uuid4

import pytest
import respx
from httpx import Response
from opentelemetry import trace

from prefect import flow
from prefect.client.orchestration import PrefectClient
from prefect.client.schemas import TaskRunResult
from prefect.client.schemas.filters import TaskRunFilter, TaskRunFilterFlowRunId
from prefect.client.schemas.objects import StateType
from prefect.context import FlowRunContext
from prefect.deployments import arun_deployment, run_deployment
from prefect.flow_engine import run_flow_async
from prefect.settings import (
    PREFECT_API_URL,
    PREFECT_UI_URL,
    temporary_settings,
)
from prefect.states import Completed, Failed
from prefect.tasks import task
from prefect.telemetry.run_telemetry import (
    LABELS_TRACEPARENT_KEY,
)
from prefect.types._datetime import now
from prefect.utilities.slugify import slugify
from tests.telemetry.instrumentation_tester import InstrumentationTester

if TYPE_CHECKING:
    from prefect.client.orchestration import PrefectClient


class TestRunDeployment:
    @pytest.fixture
    async def test_deployment(self, prefect_client: PrefectClient):
        flow_name = f"foo-{uuid4()}"
        flow_id = await prefect_client.create_flow_from_name(flow_name)

        deployment_id = await prefect_client.create_deployment(
            name=f"foo-deployment-{uuid4()}",
            flow_id=flow_id,
            parameter_openapi_schema={"type": "object", "properties": {}},
        )
        deployment = await prefect_client.read_deployment(deployment_id)

        return deployment, flow_name

    async def test_run_deployment_with_ephemeral_api(
        self, prefect_client: PrefectClient, test_deployment
    ):
        deployment, flow_name = test_deployment
        flow_run = await run_deployment(
            f"{flow_name}/{deployment.name}",
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )
        assert flow_run.deployment_id == deployment.id
        assert flow_run.state

    async def test_run_deployment_with_deployment_id_str(
        self,
        test_deployment,
        prefect_client: PrefectClient,
    ):
        deployment, _ = test_deployment

        flow_run = await run_deployment(
            f"{deployment.id}",
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )
        assert flow_run.deployment_id == deployment.id
        assert flow_run.state

    async def test_run_deployment_with_deployment_id_uuid(
        self,
        test_deployment,
        prefect_client: PrefectClient,
    ):
        deployment, _ = test_deployment

        flow_run = await run_deployment(
            deployment.id,
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )
        assert flow_run.deployment_id == deployment.id
        assert flow_run.state

    async def test_run_deployment_with_job_vars_creates_run_with_job_vars(
        self,
        test_deployment,
        prefect_client,
    ):
        # This can be removed once the flow run infra overrides is no longer an experiment
        deployment, _ = test_deployment

        job_vars = {"foo": "bar"}
        flow_run = await run_deployment(
            deployment.id,
            timeout=0,
            job_variables=job_vars,
            client=prefect_client,
        )
        assert flow_run.job_variables == job_vars
        flow_run = await prefect_client.read_flow_run(flow_run.id)
        assert flow_run.job_variables == job_vars

    async def test_returns_flow_run_on_timeout(
        self,
        test_deployment,
        use_hosted_api_server,
    ):
        deployment, flow_name = test_deployment

        mock_flowrun_response = {
            "id": str(uuid4()),
            "flow_id": str(uuid4()),
        }

        with respx.mock(
            base_url=PREFECT_API_URL.value(), assert_all_mocked=True, using="httpx"
        ) as router:
            router.get("/csrf-token", params={"client": mock.ANY}).pass_through()
            router.get(
                f"/deployments/name/{flow_name}/{deployment.name}"
            ).pass_through()
            router.post(f"/deployments/{deployment.id}/create_flow_run").pass_through()
            flow_polls = router.request(
                "GET", re.compile(PREFECT_API_URL.value() + "/flow_runs/.*")
            ).mock(
                return_value=Response(
                    200, json={**mock_flowrun_response, "state": {"type": "SCHEDULED"}}
                )
            )

            flow_run = await run_deployment(
                f"{flow_name}/{deployment.name}", timeout=1, poll_interval=0
            )
            assert len(flow_polls.calls) > 0
            assert flow_run.state

    async def test_returns_flow_run_immediately_when_timeout_is_zero(
        self,
        test_deployment,
        use_hosted_api_server,
    ):
        deployment, flow_name = test_deployment

        mock_flowrun_response = {
            "id": str(uuid4()),
            "flow_id": str(uuid4()),
        }

        with respx.mock(
            base_url=PREFECT_API_URL.value(),
            assert_all_mocked=True,
            assert_all_called=False,
            using="httpx",
        ) as router:
            router.get("/csrf-token", params={"client": mock.ANY}).pass_through()
            router.get(
                f"/deployments/name/{flow_name}/{deployment.name}"
            ).pass_through()
            router.post(f"/deployments/{deployment.id}/create_flow_run").pass_through()
            flow_polls = router.request(
                "GET", re.compile(PREFECT_API_URL.value() + "/flow_runs/.*")
            ).mock(
                return_value=Response(
                    200, json={**mock_flowrun_response, "state": {"type": "SCHEDULED"}}
                )
            )

            flow_run = await run_deployment(
                f"{flow_name}/{deployment.name}", timeout=0, poll_interval=0
            )
            assert len(flow_polls.calls) == 0
            assert flow_run.state.is_scheduled()

    async def test_returns_flow_run_from_2_dot_0(
        self,
        test_deployment,
        use_hosted_api_server,
    ):
        """
        See https://github.com/PrefectHQ/prefect/issues/15694
        """
        deployment, flow_name = test_deployment

        mock_flowrun_response = {
            "id": str(uuid4()),
            "flow_id": str(uuid4()),
        }

        side_effects = [
            Response(
                200, json={**mock_flowrun_response, "state": {"type": "SCHEDULED"}}
            )
        ]
        side_effects.append(
            Response(
                200,
                json={
                    **mock_flowrun_response,
                    "state": {"type": "COMPLETED", "data": {"type": "unpersisted"}},
                },
            )
        )

        with respx.mock(
            base_url=PREFECT_API_URL.value(),
            assert_all_mocked=True,
            assert_all_called=False,
            using="httpx",
        ) as router:
            router.get("/csrf-token", params={"client": mock.ANY}).pass_through()
            router.get(
                f"/deployments/name/{flow_name}/{deployment.name}"
            ).pass_through()
            router.post(f"/deployments/{deployment.id}/create_flow_run").pass_through()
            router.request(
                "GET", re.compile(PREFECT_API_URL.value() + "/flow_runs/.*")
            ).mock(side_effect=side_effects)

            flow_run = await run_deployment(
                f"{flow_name}/{deployment.name}", timeout=None, poll_interval=0
            )
            assert flow_run.state.is_completed()
            assert flow_run.state.data is None

    async def test_polls_indefinitely(
        self,
        test_deployment,
        use_hosted_api_server,
    ):
        deployment, flow_name = test_deployment

        mock_flowrun_response = {
            "id": str(uuid4()),
            "flow_id": str(uuid4()),
        }

        side_effects = [
            Response(
                200, json={**mock_flowrun_response, "state": {"type": "SCHEDULED"}}
            )
        ] * 99
        side_effects.append(
            Response(
                200, json={**mock_flowrun_response, "state": {"type": "COMPLETED"}}
            )
        )

        with respx.mock(
            base_url=PREFECT_API_URL.value(),
            assert_all_mocked=True,
            assert_all_called=False,
            using="httpx",
        ) as router:
            router.get("/csrf-token", params={"client": mock.ANY}).pass_through()
            router.get(
                f"/deployments/name/{flow_name}/{deployment.name}"
            ).pass_through()
            router.post(f"/deployments/{deployment.id}/create_flow_run").pass_through()
            flow_polls = router.request(
                "GET", re.compile(PREFECT_API_URL.value() + "/flow_runs/.*")
            ).mock(side_effect=side_effects)

            await run_deployment(
                f"{flow_name}/{deployment.name}", timeout=None, poll_interval=0
            )
            assert len(flow_polls.calls) == 100

    async def test_schedules_immediately_by_default(
        self, test_deployment, use_hosted_api_server
    ):
        deployment, flow_name = test_deployment

        scheduled_time = now("UTC")
        flow_run = await run_deployment(
            f"{flow_name}/{deployment.name}",
            timeout=0,
            poll_interval=0,
        )

        assert (flow_run.expected_start_time - scheduled_time).total_seconds() < 1

    async def test_accepts_custom_scheduled_time(
        self, test_deployment, use_hosted_api_server
    ):
        deployment, flow_name = test_deployment

        scheduled_time = now("UTC") + timedelta(minutes=5)
        flow_run = await run_deployment(
            f"{flow_name}/{deployment.name}",
            scheduled_time=scheduled_time,
            timeout=0,
            poll_interval=0,
        )

        assert (flow_run.expected_start_time - scheduled_time).total_seconds() < 1

    async def test_custom_flow_run_names(self, test_deployment, use_hosted_api_server):
        deployment, flow_name = test_deployment

        flow_run = await run_deployment(
            f"{flow_name}/{deployment.name}",
            flow_run_name="a custom flow run name",
            timeout=0,
            poll_interval=0,
        )

        assert flow_run.name == "a custom flow run name"

    async def test_accepts_tags(self, test_deployment):
        deployment, flow_name = test_deployment

        flow_run = await run_deployment(
            f"{flow_name}/{deployment.name}",
            tags=["I", "love", "prefect"],
            timeout=0,
            poll_interval=0,
        )

        assert sorted(flow_run.tags) == ["I", "love", "prefect"]

    async def test_accepts_idempotency_key(self, test_deployment):
        deployment, flow_name = test_deployment

        flow_run_a = await run_deployment(
            f"{flow_name}/{deployment.name}",
            idempotency_key="12345",
            timeout=0,
            poll_interval=0,
        )

        flow_run_b = await run_deployment(
            f"{flow_name}/{deployment.name}",
            idempotency_key="12345",
            timeout=0,
            poll_interval=0,
        )

        assert flow_run_a.id == flow_run_b.id

    async def test_links_to_parent_flow_run_when_used_in_flow_by_default(
        self, test_deployment, use_hosted_api_server, prefect_client: "PrefectClient"
    ):
        my_deployment, flow_name = test_deployment

        @flow(name=f"test_links_parent_flow_{uuid4()}")
        async def foo():
            return await run_deployment(
                f"{flow_name}/{my_deployment.name}",
                timeout=0,
                poll_interval=0,
            )

        parent_state = await foo(return_state=True)
        child_flow_run = await parent_state.result()
        assert child_flow_run.parent_task_run_id is not None
        task_run = await prefect_client.read_task_run(child_flow_run.parent_task_run_id)
        assert task_run.flow_run_id == parent_state.state_details.flow_run_id
        deployment_name = f"{flow_name}/{my_deployment.name}"
        assert isinstance(deployment_name, str)
        assert slugify(deployment_name) in task_run.task_key

    async def test_optionally_does_not_link_to_parent_flow_run_when_used_in_flow(
        self, test_deployment, use_hosted_api_server, prefect_client: "PrefectClient"
    ):
        deployment, flow_name = test_deployment

        @flow(name=f"test_optionally_no_link_{uuid4()}")
        async def foo():
            return await run_deployment(
                f"{flow_name}/{deployment.name}",
                timeout=0,
                poll_interval=0,
                as_subflow=False,
            )

        parent_state = await foo(return_state=True)
        child_flow_run = await parent_state.result()
        assert child_flow_run.parent_task_run_id is None

    @pytest.mark.usefixtures("use_hosted_api_server")
    async def test_links_to_parent_flow_run_when_used_in_task_without_flow_context(
        self, test_deployment, prefect_client
    ):
        """
        Regression test for deployments in a task on Dask and Ray task runners
        which do not have access to the flow run context - https://github.com/PrefectHQ/prefect/issues/9135
        """
        deployment, flow_name = test_deployment

        @task(name=f"yeet_deployment_{uuid4()}")
        async def yeet_deployment():
            with mock.patch.object(FlowRunContext, "get", return_value=None):
                assert FlowRunContext.get() is None
                result = await run_deployment(
                    f"{flow_name}/{deployment.name}",
                    timeout=0,
                    poll_interval=0,
                )
                return result

        @flow(name=f"test_links_task_no_context_{uuid4()}")
        async def foo():
            return await yeet_deployment()

        parent_state = await foo(return_state=True)
        child_flow_run = await parent_state.result()
        assert child_flow_run.parent_task_run_id is not None
        task_run = await prefect_client.read_task_run(child_flow_run.parent_task_run_id)
        assert task_run.flow_run_id == parent_state.state_details.flow_run_id
        assert slugify(f"{flow_name}/{deployment.name}") in task_run.task_key

    async def test_tracks_dependencies_when_used_in_flow(
        self, test_deployment, use_hosted_api_server, prefect_client, events_pipeline
    ):
        deployment, flow_name = test_deployment

        @task(name=f"bar_{uuid4()}")
        def bar():
            return "hello-world!!"

        @flow(name=f"test_tracks_deps_{uuid4()}")
        async def foo():
            upstream_task_state = bar(return_state=True)
            upstream_result = await upstream_task_state.result()
            child_flow_run = await run_deployment(
                f"{flow_name}/{deployment.name}",
                timeout=0,
                poll_interval=0,
                parameters={"x": upstream_result},
            )
            return upstream_task_state, child_flow_run

        parent_state = await foo(return_state=True)
        upstream_task_state, child_flow_run = await parent_state.result()
        assert child_flow_run.parent_task_run_id is not None

        task_run = await prefect_client.read_task_run(child_flow_run.parent_task_run_id)
        assert task_run.task_inputs == {
            "x": [
                TaskRunResult(
                    input_type="task_run",
                    id=upstream_task_state.state_details.task_run_id,
                )
            ]
        }

    async def test_tracks_parent_task_when_called_from_task(
        self, test_deployment, use_hosted_api_server, prefect_client
    ):
        """
        Test that when run_deployment is called from within a task,
        the wrapper task run tracks the calling task as a parent
        in task_inputs["__parents__"].

        This is important for the execution graph to correctly display
        the deployment flow run as nested under the calling task.

        Regression test for https://github.com/PrefectHQ/prefect/issues/19359
        """
        deployment, flow_name = test_deployment

        @task(name=f"trigger_deployment_{uuid4()}")
        async def trigger_deployment():
            return await run_deployment(
                f"{flow_name}/{deployment.name}",
                timeout=0,
                poll_interval=0,
            )

        @flow(name=f"parent_flow_{uuid4()}")
        async def parent_flow():
            return await trigger_deployment(return_state=True)

        parent_state = await parent_flow(return_state=True)
        calling_task_state = await parent_state.result()
        child_flow_run = await calling_task_state.result()

        # The wrapper task run should exist and be linked to the child flow run
        assert child_flow_run.parent_task_run_id is not None
        wrapper_task_run = await prefect_client.read_task_run(
            child_flow_run.parent_task_run_id
        )

        # The wrapper task run should have the calling task as its parent
        # in the __parents__ field, which enables proper execution graph display
        assert "__parents__" in wrapper_task_run.task_inputs
        assert wrapper_task_run.task_inputs["__parents__"] == [
            TaskRunResult(
                input_type="task_run",
                id=calling_task_state.state_details.task_run_id,
            )
        ]

    async def test_propagates_otel_trace_to_deployment_flow_run(
        self,
        test_deployment,
        instrumentation: InstrumentationTester,
        prefect_client: "PrefectClient",
    ):
        """Test that OTEL trace context gets propagated from parent flow to deployment flow run"""
        _, flow_name = test_deployment

        @flow(name=f"child_flow_{uuid4()}", flow_run_name="child-flow")
        async def child_flow() -> None:
            pass

        flow_id = await prefect_client.create_flow(child_flow)

        deployment_id = await prefect_client.create_deployment(
            name=f"foo-deployment-{uuid4()}",
            flow_id=flow_id,
            parameter_openapi_schema={"type": "object", "properties": {}},
        )
        deployment = await prefect_client.read_deployment(deployment_id)
        child_flow_name = child_flow.name

        @flow(name=f"parent_flow_{uuid4()}", flow_run_name="parent-flow")
        async def parent_flow():
            return await run_deployment(
                f"{child_flow_name}/{deployment.name}",
                timeout=0,
                poll_interval=0,
            )

        parent_state = await parent_flow(return_state=True)
        child_flow_run = await parent_state.result()

        await run_flow_async(child_flow, child_flow_run)
        # Get all spans
        spans = instrumentation.get_finished_spans()

        # Find parent flow span
        parent_span = next(
            span
            for span in spans
            if span.attributes.get("prefect.run.name") == "parent-flow"
        )
        child_span = next(
            span
            for span in spans
            if span.attributes.get("prefect.run.name") == "child-flow"
        )
        assert child_span
        assert parent_span

        assert child_span.parent.span_id == parent_span.get_span_context().span_id
        assert child_span.parent.trace_id == parent_span.get_span_context().trace_id

    async def test_propagates_otel_trace_from_app_to_deployment_flow_run(
        self,
        test_deployment,
        instrumentation: InstrumentationTester,
        prefect_client: "PrefectClient",
    ):
        """Test that OTEL trace context gets propagated from external app to deployment flow run"""
        deployment, flow_name = test_deployment

        @flow(name=f"foo_flow_{uuid4()}", flow_run_name="foo-flow")
        async def foo_flow() -> None:
            pass

        with trace.get_tracer("prefect-test").start_as_current_span(
            name="app-root-span"
        ):
            flow_run = await run_deployment(
                f"{flow_name}/{deployment.name}",
                timeout=0,
                poll_interval=0,
                client=prefect_client,
            )

            assert LABELS_TRACEPARENT_KEY in flow_run.labels

        # Get all spans
        spans = instrumentation.get_finished_spans()
        # Find parent flow span
        app_root_span = next(span for span in spans if span.name == "app-root-span")

        # Reset InstrumentationTester so that the app_root_span is forgotten
        instrumentation = InstrumentationTester()

        await run_flow_async(foo_flow, flow_run)

        # Get all spans
        spans = instrumentation.get_finished_spans()

        foo_span = next(
            span
            for span in spans
            if span.attributes.get("prefect.run.name") == "foo-flow"
        )
        assert foo_span
        assert app_root_span

        assert foo_span.parent
        assert foo_span.parent.span_id == app_root_span.get_span_context().span_id
        assert foo_span.parent.trace_id == app_root_span.get_span_context().trace_id


class TestArunDeployment:
    """Tests for the explicit async arun_deployment function."""

    @pytest.fixture
    async def test_deployment(self, prefect_client: PrefectClient):
        flow_name = f"foo-{uuid4()}"
        flow_id = await prefect_client.create_flow_from_name(flow_name)

        deployment_id = await prefect_client.create_deployment(
            name=f"foo-deployment-{uuid4()}",
            flow_id=flow_id,
            parameter_openapi_schema={"type": "object", "properties": {}},
        )
        deployment = await prefect_client.read_deployment(deployment_id)

        return deployment, flow_name

    async def test_arun_deployment_basic(
        self, prefect_client: PrefectClient, test_deployment
    ):
        """Test that arun_deployment can be called directly."""
        deployment, flow_name = test_deployment
        flow_run = await arun_deployment(
            f"{flow_name}/{deployment.name}",
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )
        assert flow_run.deployment_id == deployment.id
        assert flow_run.state

    async def test_arun_deployment_with_deployment_id(
        self,
        test_deployment,
        prefect_client: PrefectClient,
    ):
        """Test that arun_deployment works with deployment UUID."""
        deployment, _ = test_deployment

        flow_run = await arun_deployment(
            deployment.id,
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )
        assert flow_run.deployment_id == deployment.id
        assert flow_run.state

    async def test_arun_deployment_with_parameters(
        self,
        test_deployment,
        prefect_client: PrefectClient,
    ):
        """Test that arun_deployment passes parameters correctly."""
        deployment, flow_name = test_deployment

        flow_run = await arun_deployment(
            f"{flow_name}/{deployment.name}",
            parameters={"test_param": "test_value"},
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )
        assert flow_run.deployment_id == deployment.id
        assert flow_run.parameters == {"test_param": "test_value"}

    async def test_arun_deployment_with_tags(
        self,
        test_deployment,
        prefect_client: PrefectClient,
    ):
        """Test that arun_deployment accepts tags."""
        deployment, flow_name = test_deployment

        flow_run = await arun_deployment(
            f"{flow_name}/{deployment.name}",
            tags=["async", "test"],
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )
        assert sorted(flow_run.tags) == ["async", "test"]

    async def test_arun_deployment_with_custom_name(
        self,
        test_deployment,
        prefect_client: PrefectClient,
    ):
        """Test that arun_deployment accepts custom flow run names."""
        deployment, flow_name = test_deployment

        flow_run = await arun_deployment(
            f"{flow_name}/{deployment.name}",
            flow_run_name="custom-async-run",
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )
        assert flow_run.name == "custom-async-run"

    async def test_arun_deployment_with_job_variables(
        self,
        test_deployment,
        prefect_client: PrefectClient,
    ):
        """Test that arun_deployment passes job variables correctly."""
        deployment, _ = test_deployment

        job_vars = {"env.MY_VAR": "my_value"}
        flow_run = await arun_deployment(
            deployment.id,
            timeout=0,
            job_variables=job_vars,
            client=prefect_client,
        )
        assert flow_run.job_variables == job_vars

    async def test_arun_deployment_negative_timeout_raises(
        self,
        test_deployment,
        prefect_client: PrefectClient,
    ):
        """Test that arun_deployment raises on negative timeout."""
        deployment, flow_name = test_deployment

        with pytest.raises(ValueError, match="`timeout` cannot be negative"):
            await arun_deployment(
                f"{flow_name}/{deployment.name}",
                timeout=-1,
                client=prefect_client,
            )

    async def test_arun_deployment_idempotency_key(
        self,
        test_deployment,
        prefect_client: PrefectClient,
    ):
        """Test that arun_deployment respects idempotency keys."""
        deployment, flow_name = test_deployment

        flow_run_a = await arun_deployment(
            f"{flow_name}/{deployment.name}",
            idempotency_key="async-12345",
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )

        flow_run_b = await arun_deployment(
            f"{flow_name}/{deployment.name}",
            idempotency_key="async-12345",
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )

        assert flow_run_a.id == flow_run_b.id

    async def test_arun_deployment_links_to_parent_flow(
        self,
        test_deployment,
        use_hosted_api_server,
        prefect_client: PrefectClient,
    ):
        """Test that arun_deployment links to parent flow when called from within a flow."""
        my_deployment, flow_name = test_deployment

        @flow(name=f"parent_flow_{uuid4()}")
        async def parent_flow():
            return await arun_deployment(
                f"{flow_name}/{my_deployment.name}",
                timeout=0,
                poll_interval=0,
            )

        parent_state = await parent_flow(return_state=True)
        child_flow_run = await parent_state.result()
        assert child_flow_run.parent_task_run_id is not None
        task_run = await prefect_client.read_task_run(child_flow_run.parent_task_run_id)
        assert task_run.flow_run_id == parent_state.state_details.flow_run_id


class TestDedupPlaceholderMirroring:
    """
    Tests for labeling the placeholder task run that is orphaned when
    `create_flow_run_from_deployment` deduplicates on an idempotency key.

    On a dedup hit the returned flow run's `parent_task_run_id` points at the
    *original* call's placeholder, so the placeholder created by this call is
    never updated by the `UpdateSubflowParentTask` orchestration policy and
    would remain `Pending` forever. These tests pin the labeling behavior.
    """

    @pytest.fixture
    async def test_deployment(self, prefect_client: PrefectClient):
        flow_name = f"dedup-{uuid4()}"
        flow_id = await prefect_client.create_flow_from_name(flow_name)

        deployment_id = await prefect_client.create_deployment(
            name=f"dedup-deployment-{uuid4()}",
            flow_id=flow_id,
            parameter_openapi_schema={"type": "object", "properties": {}},
        )
        deployment = await prefect_client.read_deployment(deployment_id)

        return deployment, flow_name

    async def _read_deployment_placeholders(
        self, prefect_client: PrefectClient, parent_flow_run_id
    ):
        task_runs = await prefect_client.read_task_runs(
            task_run_filter=TaskRunFilter(
                flow_run_id=TaskRunFilterFlowRunId(any_=[parent_flow_run_id])
            )
        )
        return [
            task_run
            for task_run in task_runs
            if task_run.task_key.startswith(
                "prefect.deployments.flow_runs.run_deployment."
            )
        ]

    async def _read_orphan(
        self, prefect_client: PrefectClient, parent_flow_run_id, first_placeholder_id
    ):
        placeholders = await self._read_deployment_placeholders(
            prefect_client, parent_flow_run_id
        )
        assert len(placeholders) == 2
        return next(
            task_run for task_run in placeholders if task_run.id != first_placeholder_id
        )

    async def test_failed_duplicate_mirrors_onto_second_placeholder(
        self,
        test_deployment,
        use_hosted_api_server,
        prefect_client: PrefectClient,
    ):
        deployment, flow_name = test_deployment
        idempotency_key = f"dedup-failed-{uuid4()}"
        first_placeholder_ids = {}

        @flow(name=f"parent-{uuid4()}")
        async def parent():
            first = await arun_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=0,
                poll_interval=0,
                client=prefect_client,
            )
            first_placeholder_ids["value"] = first.parent_task_run_id
            # Terminal-fail the run so the dedup hit returns a Failed duplicate
            await prefect_client.set_flow_run_state(
                first.id, Failed(message="original failure"), force=True
            )
            second = await arun_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=0,
                poll_interval=0,
                client=prefect_client,
            )
            return first, second

        with temporary_settings({PREFECT_UI_URL: "http://test/ui"}):
            parent_state = await parent(return_state=True)
        first, second = await parent_state.result()
        assert first.id == second.id

        orphan = await self._read_orphan(
            prefect_client,
            parent_state.state_details.flow_run_id,
            first_placeholder_ids["value"],
        )
        assert orphan.state.type == StateType.FAILED
        assert idempotency_key in orphan.state.message
        assert str(first.id) in orphan.state.message
        assert f"http://test/ui/runs/flow-run/{first.id}" in orphan.state.message
        assert orphan.state.state_details.child_flow_run_id == first.id

    async def test_completed_duplicate_mirrors_onto_second_placeholder(
        self,
        test_deployment,
        use_hosted_api_server,
        prefect_client: PrefectClient,
    ):
        deployment, flow_name = test_deployment
        idempotency_key = f"dedup-completed-{uuid4()}"
        first_placeholder_ids = {}

        @flow(name=f"parent-{uuid4()}")
        async def parent():
            first = await arun_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=0,
                poll_interval=0,
                client=prefect_client,
            )
            first_placeholder_ids["value"] = first.parent_task_run_id
            await prefect_client.set_flow_run_state(
                first.id, Completed(message="original success"), force=True
            )
            second = await arun_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=0,
                poll_interval=0,
                client=prefect_client,
            )
            return first, second

        parent_state = await parent(return_state=True)
        first, second = await parent_state.result()
        assert first.id == second.id

        orphan = await self._read_orphan(
            prefect_client,
            parent_state.state_details.flow_run_id,
            first_placeholder_ids["value"],
        )
        assert orphan.state.type == StateType.COMPLETED
        assert idempotency_key in orphan.state.message
        assert orphan.state.state_details.child_flow_run_id == first.id

    async def test_fresh_create_gets_no_client_state_write(
        self,
        test_deployment,
        use_hosted_api_server,
        prefect_client: PrefectClient,
    ):
        """
        On a fresh (non-dedup) create, the client must not write placeholder
        state: the server's UpdateSubflowParentTask policy mirrors the child's
        state onto the placeholder. The placeholder therefore advances beyond
        its initial PENDING without any client-side write.
        """
        deployment, flow_name = test_deployment
        idempotency_key = f"fresh-{uuid4()}"

        @flow(name=f"parent-{uuid4()}")
        async def parent():
            return await arun_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=0,
                poll_interval=0,
                client=prefect_client,
            )

        with mock.patch.object(
            prefect_client,
            "set_task_run_state",
            wraps=prefect_client.set_task_run_state,
        ) as spy:
            parent_state = await parent(return_state=True)

        run = await parent_state.result()
        assert run.parent_task_run_id is not None
        assert spy.call_count == 0

        placeholders = await self._read_deployment_placeholders(
            prefect_client, parent_state.state_details.flow_run_id
        )
        assert len(placeholders) == 1
        assert placeholders[0].id == run.parent_task_run_id
        # The child's initial Scheduled state is mirrored server-side.
        assert placeholders[0].state.type == StateType.SCHEDULED

    async def test_nonterminal_duplicate_left_pending_on_timeout_zero(
        self,
        test_deployment,
        use_hosted_api_server,
        prefect_client: PrefectClient,
    ):
        deployment, flow_name = test_deployment
        idempotency_key = f"dedup-nonterminal-{uuid4()}"
        first_placeholder_ids = {}

        @flow(name=f"parent-{uuid4()}")
        async def parent():
            first = await arun_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=0,
                poll_interval=0,
                client=prefect_client,
            )
            first_placeholder_ids["value"] = first.parent_task_run_id
            # Duplicate is still Scheduled: the placeholder must not be
            # labeled with a terminal state it does not have yet.
            second = await arun_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=0,
                poll_interval=0,
                client=prefect_client,
            )
            return first, second

        parent_state = await parent(return_state=True)
        first, second = await parent_state.result()
        assert first.id == second.id

        orphan = await self._read_orphan(
            prefect_client,
            parent_state.state_details.flow_run_id,
            first_placeholder_ids["value"],
        )
        assert orphan.state.type == StateType.PENDING

    async def test_poll_exit_mirrors_final_state_of_dedup_duplicate(
        self,
        test_deployment,
        use_hosted_api_server,
        prefect_client: PrefectClient,
    ):
        deployment, flow_name = test_deployment
        idempotency_key = f"dedup-poll-{uuid4()}"

        # First dispatch outside a flow: creates the run that later dedups.
        first = await arun_deployment(
            f"{flow_name}/{deployment.name}",
            idempotency_key=idempotency_key,
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )
        real = await prefect_client.read_flow_run(first.id)
        base_payload = real.model_dump(mode="json")
        scheduled_payload = {**base_payload, "state": {"type": "SCHEDULED"}}
        completed_payload = {**base_payload, "state": {"type": "COMPLETED"}}

        @flow(name=f"parent-{uuid4()}")
        async def parent():
            return await arun_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=None,
                poll_interval=0,
                client=prefect_client,
            )

        with respx.mock(
            base_url=PREFECT_API_URL.value(),
            assert_all_mocked=True,
            assert_all_called=False,
            using="httpx",
        ) as router:
            # Real server handles creation (and the dedup); only the polls are
            # scripted so the duplicate is observed as non-terminal first.
            flow_polls = router.request(
                "GET", re.compile(PREFECT_API_URL.value() + "/flow_runs/.*")
            ).mock(
                side_effect=[
                    Response(200, json=scheduled_payload),
                    Response(200, json=completed_payload),
                ]
            )
            router.route().pass_through()

            parent_state = await parent(return_state=True)

        second = await parent_state.result()
        assert second.id == first.id
        assert len(flow_polls.calls) == 2

        placeholders = await self._read_deployment_placeholders(
            prefect_client, parent_state.state_details.flow_run_id
        )
        assert len(placeholders) == 1
        orphan = placeholders[0]
        assert orphan.state.type == StateType.COMPLETED
        assert idempotency_key in orphan.state.message
        assert orphan.state.state_details.child_flow_run_id == first.id

    async def test_as_subflow_false_creates_no_placeholder(
        self,
        test_deployment,
        use_hosted_api_server,
        prefect_client: PrefectClient,
    ):
        deployment, flow_name = test_deployment
        idempotency_key = f"dedup-nosubflow-{uuid4()}"

        @flow(name=f"parent-{uuid4()}")
        async def parent():
            first = await arun_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=0,
                poll_interval=0,
                as_subflow=False,
                client=prefect_client,
            )
            second = await arun_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=0,
                poll_interval=0,
                as_subflow=False,
                client=prefect_client,
            )
            return first, second

        with mock.patch.object(
            prefect_client,
            "set_task_run_state",
            wraps=prefect_client.set_task_run_state,
        ) as spy:
            parent_state = await parent(return_state=True)

        first, second = await parent_state.result()
        assert first.parent_task_run_id is None
        assert second.parent_task_run_id is None
        assert spy.call_count == 0

        placeholders = await self._read_deployment_placeholders(
            prefect_client, parent_state.state_details.flow_run_id
        )
        assert len(placeholders) == 0

    @pytest.fixture
    def test_deployment_sync(self, sync_prefect_client):
        flow_name = f"dedup-sync-{uuid4()}"
        flow_id = sync_prefect_client.create_flow_from_name(flow_name)

        deployment_id = sync_prefect_client.create_deployment(
            name=f"dedup-sync-deployment-{uuid4()}",
            flow_id=flow_id,
            parameter_openapi_schema={"type": "object", "properties": {}},
        )
        deployment = sync_prefect_client.read_deployment(deployment_id)

        return deployment, flow_name

    def test_sync_run_deployment_mirrors_failed_duplicate(
        self,
        sync_prefect_client,
        test_deployment_sync,
    ):
        deployment, flow_name = test_deployment_sync
        idempotency_key = f"dedup-sync-{uuid4()}"
        first_placeholder_ids = {}

        @flow(name=f"sync-parent-{uuid4()}")
        def parent():
            first = run_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=0,
                poll_interval=0,
                _sync=True,
            )
            first_placeholder_ids["value"] = first.parent_task_run_id
            sync_prefect_client.set_flow_run_state(
                first.id, Failed(message="original failure"), force=True
            )
            second = run_deployment(
                f"{flow_name}/{deployment.name}",
                idempotency_key=idempotency_key,
                timeout=0,
                poll_interval=0,
                _sync=True,
            )
            return first, second

        parent_state = parent(return_state=True)
        first, second = parent_state.result()
        assert first.id == second.id

        task_runs = sync_prefect_client.read_task_runs(
            task_run_filter=TaskRunFilter(
                flow_run_id=TaskRunFilterFlowRunId(
                    any_=[parent_state.state_details.flow_run_id]
                )
            )
        )
        placeholders = [
            task_run
            for task_run in task_runs
            if task_run.task_key.startswith(
                "prefect.deployments.flow_runs.run_deployment."
            )
        ]
        assert len(placeholders) == 2
        orphan = next(
            task_run
            for task_run in placeholders
            if task_run.id != first_placeholder_ids["value"]
        )
        assert orphan.state.type == StateType.FAILED
        assert idempotency_key in orphan.state.message
        assert orphan.state.state_details.child_flow_run_id == first.id


class TestRunDeploymentSyncContext:
    """Tests for run_deployment behavior in sync context."""

    @pytest.fixture
    def test_deployment_sync(self, sync_prefect_client):
        flow_name = f"foo-sync-{uuid4()}"
        flow_id = sync_prefect_client.create_flow_from_name(flow_name)

        deployment_id = sync_prefect_client.create_deployment(
            name=f"foo-sync-deployment-{uuid4()}",
            flow_id=flow_id,
            parameter_openapi_schema={"type": "object", "properties": {}},
        )
        deployment = sync_prefect_client.read_deployment(deployment_id)

        return deployment, flow_name

    def test_run_deployment_sync_basic(
        self,
        sync_prefect_client,
        test_deployment_sync,
    ):
        """Test that run_deployment works in a synchronous context."""
        deployment, flow_name = test_deployment_sync
        # Force sync execution using _sync parameter
        flow_run = run_deployment(
            f"{flow_name}/{deployment.name}",
            timeout=0,
            poll_interval=0,
            _sync=True,
        )
        assert flow_run.deployment_id == deployment.id
        assert flow_run.state

    def test_run_deployment_sync_with_parameters(
        self,
        sync_prefect_client,
        test_deployment_sync,
    ):
        """Test that run_deployment in sync context passes parameters correctly."""
        deployment, flow_name = test_deployment_sync

        flow_run = run_deployment(
            f"{flow_name}/{deployment.name}",
            parameters={"sync_param": "sync_value"},
            timeout=0,
            poll_interval=0,
            _sync=True,
        )
        assert flow_run.deployment_id == deployment.id
        assert flow_run.parameters == {"sync_param": "sync_value"}

    def test_run_deployment_sync_with_tags(
        self,
        sync_prefect_client,
        test_deployment_sync,
    ):
        """Test that run_deployment in sync context accepts tags."""
        deployment, flow_name = test_deployment_sync

        flow_run = run_deployment(
            f"{flow_name}/{deployment.name}",
            tags=["sync", "test"],
            timeout=0,
            poll_interval=0,
            _sync=True,
        )
        assert sorted(flow_run.tags) == ["sync", "test"]

    def test_run_deployment_sync_negative_timeout_raises(
        self,
        sync_prefect_client,
        test_deployment_sync,
    ):
        """Test that run_deployment in sync context raises on negative timeout."""
        deployment, flow_name = test_deployment_sync

        with pytest.raises(ValueError, match="`timeout` cannot be negative"):
            run_deployment(
                f"{flow_name}/{deployment.name}",
                timeout=-1,
                _sync=True,
            )

    def test_run_deployment_sync_with_deployment_id(
        self,
        sync_prefect_client,
        test_deployment_sync,
    ):
        """Test that run_deployment in sync context works with deployment UUID."""
        deployment, _ = test_deployment_sync

        flow_run = run_deployment(
            deployment.id,
            timeout=0,
            poll_interval=0,
            _sync=True,
        )
        assert flow_run.deployment_id == deployment.id
        assert flow_run.state


class TestAsyncDispatchBehavior:
    """Tests to verify async_dispatch routes correctly between sync and async."""

    @pytest.fixture
    async def test_deployment(self, prefect_client: PrefectClient):
        flow_name = f"dispatch-test-{uuid4()}"
        flow_id = await prefect_client.create_flow_from_name(flow_name)

        deployment_id = await prefect_client.create_deployment(
            name=f"dispatch-deployment-{uuid4()}",
            flow_id=flow_id,
            parameter_openapi_schema={"type": "object", "properties": {}},
        )
        deployment = await prefect_client.read_deployment(deployment_id)

        return deployment, flow_name

    async def test_run_deployment_dispatches_to_async_in_async_context(
        self,
        prefect_client: PrefectClient,
        test_deployment,
    ):
        """Test that run_deployment returns a coroutine when called in async context."""
        deployment, flow_name = test_deployment

        # When called without await, run_deployment should return a coroutine in async context
        result = run_deployment(
            f"{flow_name}/{deployment.name}",
            timeout=0,
            poll_interval=0,
            client=prefect_client,
        )

        # Verify it's a coroutine
        assert inspect.iscoroutine(result)

        # Now await it
        flow_run = await result
        assert flow_run.deployment_id == deployment.id

    async def test_run_deployment_aio_attribute(
        self,
        prefect_client: PrefectClient,
        test_deployment,
    ):
        """Test that run_deployment.aio attribute references arun_deployment."""
        # The .aio attribute should be available for backward compatibility
        assert hasattr(run_deployment, "aio")
        assert run_deployment.aio is arun_deployment
