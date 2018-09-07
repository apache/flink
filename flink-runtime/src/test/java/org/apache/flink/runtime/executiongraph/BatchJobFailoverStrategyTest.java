package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.testutils.ManuallyTriggeredDirectExecutor;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.BatchJobFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.executiongraph.utils.SimpleSlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.concurrent.Executor;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilExecutionState;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.waitUntilJobStatus;
import static org.junit.Assert.assertEquals;

/**
 * The BatchJobFailoverStrategy is based on PipelinedRegionStrategy, the tests will focus on extended functionalities
 * */
public class BatchJobFailoverStrategyTest extends TestLogger {

	/**
	 * V1-->V2|-->V3-->V4, v1,v2 is in one region, v3,v4 in another region
	 * */
	private ExecutionGraph createSampleExampleGraph(ManuallyTriggeredDirectExecutor executor) throws Exception {

		final JobID jobId = new JobID();
		final SimpleSlotProvider slotProvider = new SimpleSlotProvider(jobId, 10);

		JobVertex v1 = new JobVertex("vertex1");
		JobVertex v2 = new JobVertex("vertex2");
		JobVertex v3 = new JobVertex("vertex3");
		JobVertex v4 = new JobVertex("vertex4");

		v1.setParallelism(1);
		v2.setParallelism(1);
		v3.setParallelism(1);
		v4.setParallelism(1);

		v1.setInvokableClass(NoOpInvokable.class);
		v2.setInvokableClass(NoOpInvokable.class);
		v3.setInvokableClass(NoOpInvokable.class);
		v4.setInvokableClass(NoOpInvokable.class);

		v2.connectNewDataSetAsInput(v1, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
		v3.connectNewDataSetAsInput(v2, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);
		v4.connectNewDataSetAsInput(v3, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobInformation jobInformation = new DummyJobInformation(jobId,"test job");

		final ExecutionGraph executionGraph = new ExecutionGraph(
			jobInformation,
			TestingUtils.defaultExecutor(),
			TestingUtils.defaultExecutor(),
			Time.seconds(10L),
			new NoRestartStrategy(),
			new BatchFailoverWithCustomExecutor(executor),
			slotProvider,
			getClass().getClassLoader(),
			VoidBlobWriter.getInstance(),
			Time.seconds(10L));

		JobGraph jg = new JobGraph(jobId, "testjob", v1, v2, v3, v4);
		executionGraph.attachJobGraph(jg.getVerticesSortedTopologicallyFromSources());

		((SimpleAckingTaskManagerGateway)slotProvider.getTaskManagerGateway()).setCancelConsumer((ExecutionAttemptID id) ->{
			TaskExecutionState state = new TaskExecutionState(jobId, id, ExecutionState.CANCELED);
			executionGraph.updateState(state);
		});

		return executionGraph;
	}

	@Test
	public void testNonRecoverableFailure() throws Exception {

		final ManuallyTriggeredDirectExecutor executor = new ManuallyTriggeredDirectExecutor();

		final ExecutionGraph graph = createSampleExampleGraph(executor);

		BatchJobFailoverStrategy strategy = (BatchJobFailoverStrategy)graph.getFailoverStrategy();

		final ExecutionJobVertex ejv = graph.getVerticesTopologically().iterator().next();
		final ExecutionVertex vertex1 = ejv.getTaskVertices()[0];

		graph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, graph.getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(vertex1).getState());

		//fail with a normal exception, job will keep on running with failover
		vertex1.getCurrentExecutionAttempt().fail(new Exception("test failure"));
		assertEquals(ExecutionState.FAILED, vertex1.getCurrentExecutionAttempt().getState());
		assertEquals(JobStatus.RUNNING, graph.getState());
		executor.trigger();

		//failed with non-recoverable error, job will fail
		vertex1.getCurrentExecutionAttempt().fail(new NoResourceAvailableException());
		assertEquals(ExecutionState.FAILED, vertex1.getCurrentExecutionAttempt().getState());
		waitUntilJobStatus(graph, JobStatus.FAILED, 2000);
	}

	@Test
	public void testFailureExceedMaxAttempt() throws Exception {
		final ManuallyTriggeredDirectExecutor executor = new ManuallyTriggeredDirectExecutor();
		final ExecutionGraph graph = createSampleExampleGraph(executor);

		BatchJobFailoverStrategy strategy = (BatchJobFailoverStrategy) graph.getFailoverStrategy();

		final ExecutionJobVertex ejv = graph.getVerticesTopologically().iterator().next();
		final ExecutionVertex vertex1 = ejv.getTaskVertices()[0];

		graph.scheduleForExecution();
		assertEquals(JobStatus.RUNNING, graph.getState());
		assertEquals(JobStatus.RUNNING, strategy.getFailoverRegion(vertex1).getState());

		int failLimit = graph.getJobConfiguration()
			.getInteger(JobManagerOptions.MAX_ATTEMPTS_EXECUTION_FAILURE_COUNT);

		for (int i = 0; i < failLimit; i++) {
			//fail with a normal exception, job will keep on running with failover
			Execution current = vertex1.getCurrentExecutionAttempt();
			waitUntilExecutionState(current, ExecutionState.DEPLOYING, 1000);
			current.fail(new Exception("test failure"));
			waitUntilExecutionState(current, ExecutionState.FAILED, 1000);
			assertEquals(JobStatus.RUNNING, graph.getState());
			executor.trigger();
		}

		//exceed limit, job will fail
		vertex1.getCurrentExecutionAttempt().fail(new Exception("test failure"));
		assertEquals(ExecutionState.FAILED, vertex1.getCurrentExecutionAttempt().getState());
		waitUntilJobStatus(graph, JobStatus.FAILED, 3000);
	}

	private static class BatchFailoverWithCustomExecutor implements FailoverStrategy.Factory {

		private final Executor executor;

		BatchFailoverWithCustomExecutor(Executor executor) {
			this.executor = executor;
		}

		@Override
		public FailoverStrategy create(ExecutionGraph executionGraph) {
			return new BatchJobFailoverStrategy(executionGraph, executor);
		}
	}
}
