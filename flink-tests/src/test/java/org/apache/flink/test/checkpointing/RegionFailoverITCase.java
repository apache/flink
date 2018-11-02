/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.apache.commons.collections.CollectionUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

/**
 * Tests for region failover with multi regions.
 */
public class RegionFailoverITCase extends TestLogger {

	private static final int FAIL_BASE = 1000;
	private static final int NUM_OF_REGIONS = 3;
	private static final Set<Integer> EXPECTED_INDICES = IntStream.range(0, NUM_OF_REGIONS).boxed().collect(Collectors.toSet());
	private static final int NUM_OF_RESTARTS = 3;
	private static final int NUM_ELEMENTS = FAIL_BASE * 10;

	private static volatile long lastCompletedCheckpointId = 0;
	private static volatile int numCompletedCheckpoints = 0;
	private static volatile AtomicInteger jobFailedCnt = new AtomicInteger(0);

	private static Map<Long, Integer> snapshotIndicesOfSubTask = new HashMap<>();

	private static MiniClusterWithClientResource cluster;

	private static boolean restoredState = false;

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	@Before
	public void setup() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");

		cluster = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
				.setConfiguration(configuration)
				.setNumberTaskManagers(2)
				.setNumberSlotsPerTaskManager(2).build());
		cluster.before();
		jobFailedCnt.set(0);
		numCompletedCheckpoints = 0;
	}

	@AfterClass
	public static void shutDownExistingCluster() {
		if (cluster != null) {
			cluster.after();
			cluster = null;
		}
	}

	/**
	 * Tests that a simple job (Source -> Map) with multi regions could restore with operator state.
	 *
	 * <p>The last subtask of Map function in the 1st stream graph would fail {@code NUM_OF_RESTARTS} times,
	 * and it will verify whether the restored state is identical to last completed checkpoint's.
	 */
	@Test(timeout = 60000)
	public void testMultiRegionFailover() {
		try {
			JobGraph jobGraph = createJobGraph();
			ClusterClient<?> client = cluster.getClusterClient();
			client.submitJob(jobGraph, RegionFailoverITCase.class.getClassLoader());
			Assert.assertTrue("The test multi-region job has never ever restored state.", restoredState);
		}
		catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	private JobGraph createJobGraph() {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(NUM_OF_REGIONS);
		env.enableCheckpointing(200, CheckpointingMode.EXACTLY_ONCE);
		env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		env.disableOperatorChaining();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(NUM_OF_RESTARTS, 0L));
		env.getConfig().disableSysoutLogging();

		// there exists num of 'NUM_OF_REGIONS' individual regions.
		env.addSource(new StringGeneratingSourceFunction(NUM_ELEMENTS, NUM_ELEMENTS / NUM_OF_RESTARTS))
			.setParallelism(NUM_OF_REGIONS)
			.map(new FailingMapperFunction(NUM_OF_RESTARTS))
			.setParallelism(NUM_OF_REGIONS);

		// another stream graph totally disconnected with the above one.
		env.addSource(new StringGeneratingSourceFunction(NUM_ELEMENTS, NUM_ELEMENTS / NUM_OF_RESTARTS)).setParallelism(1)
			.map((MapFunction<Integer, Object>) value -> value).setParallelism(1);

		return env.getStreamGraph().getJobGraph();
	}

	private static class StringGeneratingSourceFunction extends RichParallelSourceFunction<Integer>
		implements CheckpointListener, CheckpointedFunction {
		private static final long serialVersionUID = 1L;

		private final long numElements;
		private final long checkpointLatestAt;

		private int index = -1;

		private int lastRegionIndex = -1;

		private volatile boolean isRunning = true;

		private ListState<Integer> listState;

		private static final ListStateDescriptor<Integer> stateDescriptor = new ListStateDescriptor<>("list-1", Integer.class);

		private ListState<Integer> unionListState;

		private static final ListStateDescriptor<Integer> unionStateDescriptor = new ListStateDescriptor<>("list-2", Integer.class);

		StringGeneratingSourceFunction(long numElements, long checkpointLatestAt) {
			this.numElements = numElements;
			this.checkpointLatestAt = checkpointLatestAt;
		}

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
			if (index < 0) {
				// not been restored, so initialize
				index = 0;
			}

			while (isRunning && index < numElements) {
				//noinspection SynchronizationOnLocalVariableOrMethodParameter
				synchronized (ctx.getCheckpointLock()) {
					index += 1;
					ctx.collect(index);
				}

				if (numCompletedCheckpoints < 3) {
					// not yet completed enough checkpoints, so slow down
					if (index < checkpointLatestAt) {
						// mild slow down
						Thread.sleep(1);
					} else {
						// wait until the checkpoints are completed
						while (isRunning && numCompletedCheckpoints < 3) {
							Thread.sleep(300);
						}
					}
				}
				if (jobFailedCnt.get() < NUM_OF_RESTARTS) {
					// slow down if job has not failed for 'NUM_OF_RESTARTS' times.
					Thread.sleep(1);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			if (getRuntimeContext().getIndexOfThisSubtask() == NUM_OF_REGIONS - 1) {
				lastCompletedCheckpointId = checkpointId;
				snapshotIndicesOfSubTask.put(checkpointId, lastRegionIndex);
				numCompletedCheckpoints++;
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
			if (indexOfThisSubtask != 0) {
				listState.clear();
				listState.add(index);
				if (indexOfThisSubtask == NUM_OF_REGIONS - 1) {
					lastRegionIndex = index;
				}
			}
			unionListState.clear();
			unionListState.add(indexOfThisSubtask);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
			if (context.isRestored()) {
				restoredState = true;

				unionListState = context.getOperatorStateStore().getUnionListState(unionStateDescriptor);
				Set<Integer> actualIndices = StreamSupport.stream(unionListState.get().spliterator(), false).collect(Collectors.toSet());
				Assert.assertTrue(CollectionUtils.isEqualCollection(EXPECTED_INDICES, actualIndices));

				if (indexOfThisSubtask == 0) {
					listState = context.getOperatorStateStore().getListState(stateDescriptor);
					Assert.assertTrue("list state should be empty for subtask-0",
						((List<Integer>) listState.get()).isEmpty());
				} else {
					listState = context.getOperatorStateStore().getListState(stateDescriptor);
					Assert.assertTrue("list state should not be empty for subtask-" + indexOfThisSubtask,
						((List<Integer>) listState.get()).size() > 0);

					if (indexOfThisSubtask == NUM_OF_REGIONS - 1) {
						index = listState.get().iterator().next();
						if (index != snapshotIndicesOfSubTask.get(lastCompletedCheckpointId)) {
							throw new RuntimeException("Test failed due to unexpected recovered index: " + index +
								", while last completed checkpoint record index: " + snapshotIndicesOfSubTask.get(lastCompletedCheckpointId));
						}
					}
				}
			} else {
				unionListState = context.getOperatorStateStore().getUnionListState(unionStateDescriptor);

				if (indexOfThisSubtask != 0) {
					listState = context.getOperatorStateStore().getListState(stateDescriptor);
				}
			}

		}
	}

	private static class FailingMapperFunction extends RichMapFunction<Integer, Integer> {
		private final int restartTimes;

		FailingMapperFunction(int restartTimes) {
			this.restartTimes = restartTimes;
		}

		@Override
		public Integer map(Integer input) throws Exception {
			int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

			if (input > FAIL_BASE * (jobFailedCnt.get() + 1)) {

				// we would let region-0 to failover first
				if (jobFailedCnt.get() < 1 && indexOfThisSubtask == 0) {
					jobFailedCnt.incrementAndGet();
					throw new TestException();
				}

				// then let last region to failover
				if (jobFailedCnt.get() < restartTimes && indexOfThisSubtask == NUM_OF_REGIONS - 1) {
					jobFailedCnt.incrementAndGet();
					throw new TestException();
				}
			}
			return input;
		}
	}

	private static class TestException extends IOException{
		private static final long serialVersionUID = 1L;
	}
}
