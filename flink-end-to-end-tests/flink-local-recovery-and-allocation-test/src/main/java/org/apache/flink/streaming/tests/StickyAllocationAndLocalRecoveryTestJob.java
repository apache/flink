/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.tests;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Automatic end-to-end test for local recovery (including sticky allocation).
 *
 * <p>List of possible input parameters for this job:
 * <ul>
 * 	<li>checkpointDir: the checkpoint directory, required.</li>
 * 	<li>parallelism: the parallelism of the job, default 1.</li>
 *	<li>maxParallelism: the maximum parallelism of the job, default 1.</li>
 * 	<li>checkpointInterval: the checkpointing interval in milliseconds, default 1000.</li>
 * 	<li>restartDelay: the delay of the fixed delay restart strategy, default 0.</li>
 * 	<li>externalizedCheckpoints: flag to activate externalized checkpoints, default <code>false</code>.</li>
 * 	<li>stateBackend: choice for state backend between <code>file</code> and <code>rocks</code>, default <code>file</code>.</li>
 * 	<li>killJvmOnFail: flag that determines whether or not an artificial failure induced by the test kills the JVM or not.</li>
 * 	<li>asyncCheckpoints: flag for async checkpoints with file state backend, default <code>true</code>.</li>
 * 	<li>incrementalCheckpoints: flag for incremental checkpoint with rocks state backend, default <code>false</code>.</li>
 * 	<li>delay: sleep delay to throttle down the production of the source, default 0.</li>
 * 	<li>maxAttempts: the maximum number of run attempts, before the job finishes with success, default 3.</li>
 * 	<li>valueSize: size of the artificial value for each key in bytes, default 10.</li>
 * </ul>
 */
public class StickyAllocationAndLocalRecoveryTestJob {

	private static final Logger LOG = LoggerFactory.getLogger(StickyAllocationAndLocalRecoveryTestJob.class);

	public static void main(String[] args) throws Exception {

		final ParameterTool pt = ParameterTool.fromArgs(args);

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(pt.getInt("parallelism", 1));
		env.setMaxParallelism(pt.getInt("maxParallelism", pt.getInt("parallelism", 1)));
		env.enableCheckpointing(pt.getInt("checkpointInterval", 1000));
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, pt.getInt("restartDelay", 0)));
		if (pt.getBoolean("externalizedCheckpoints", false)) {
			env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
		}

		String stateBackend = pt.get("stateBackend", "file");
		String checkpointDir = pt.getRequired("checkpointDir");

		boolean killJvmOnFail = pt.getBoolean("killJvmOnFail", false);

		if ("file".equals(stateBackend)) {
			boolean asyncCheckpoints = pt.getBoolean("asyncCheckpoints", true);
			env.setStateBackend(new FsStateBackend(checkpointDir, asyncCheckpoints));
		} else if ("rocks".equals(stateBackend)) {
			boolean incrementalCheckpoints = pt.getBoolean("incrementalCheckpoints", false);
			env.setStateBackend(new RocksDBStateBackend(checkpointDir, incrementalCheckpoints));
		} else {
			throw new IllegalArgumentException("Unknown backend: " + stateBackend);
		}

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(pt);

		// delay to throttle down the production of the source
		long delay = pt.getLong("delay", 0L);

		// the maximum number of attempts, before the job finishes with success
		int maxAttempts = pt.getInt("maxAttempts", 3);

		// size of one artificial value
		int valueSize = pt.getInt("valueSize", 10);

		env.addSource(new RandomLongSource(maxAttempts, delay))
			.keyBy((KeySelector<Long, Long>) aLong -> aLong)
			.flatMap(new StateCreatingFlatMap(valueSize, killJvmOnFail))
			.addSink(new PrintSinkFunction<>());

		env.execute("Sticky Allocation And Local Recovery Test");
	}

	/**
	 * Source function that produces a long sequence.
	 */
	private static final class RandomLongSource extends RichParallelSourceFunction<Long> implements CheckpointedFunction {

		private static final long serialVersionUID = 1L;

		/**
		 * Generator delay between two events.
		 */
		final long delay;

		/**
		 * Maximum restarts before shutting down this source.
		 */
		final int maxAttempts;

		/**
		 * State that holds the current key for recovery.
		 */
		transient ListState<Long> sourceCurrentKeyState;

		/**
		 * Generator's current key.
		 */
		long currentKey;

		/**
		 * Generator runs while this is true.
		 */
		volatile boolean running;

		RandomLongSource(int maxAttempts, long delay) {
			this.delay = delay;
			this.maxAttempts = maxAttempts;
			this.running = true;
		}

		@Override
		public void run(SourceContext<Long> sourceContext) throws Exception {

			int numberOfParallelSubtasks = getRuntimeContext().getNumberOfParallelSubtasks();
			int subtaskIdx = getRuntimeContext().getIndexOfThisSubtask();

			// the source emits one final event and shuts down once we have reached max attempts.
			if (getRuntimeContext().getAttemptNumber() > maxAttempts) {
				synchronized (sourceContext.getCheckpointLock()) {
					sourceContext.collect(Long.MAX_VALUE - subtaskIdx);
				}
				return;
			}

			while (running) {

				synchronized (sourceContext.getCheckpointLock()) {
					sourceContext.collect(currentKey);
					currentKey += numberOfParallelSubtasks;
				}

				if (delay > 0) {
					Thread.sleep(delay);
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			sourceCurrentKeyState.clear();
			sourceCurrentKeyState.add(currentKey);
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {

			ListStateDescriptor<Long> currentKeyDescriptor = new ListStateDescriptor<>("currentKey", Long.class);
			sourceCurrentKeyState = context.getOperatorStateStore().getListState(currentKeyDescriptor);

			currentKey = getRuntimeContext().getIndexOfThisSubtask();
			Iterable<Long> iterable = sourceCurrentKeyState.get();
			if (iterable != null) {
				Iterator<Long> iterator = iterable.iterator();
				if (iterator.hasNext()) {
					currentKey = iterator.next();
					Preconditions.checkState(!iterator.hasNext());
				}
			}
		}
	}

	/**
	 * Stateful map function. Failure creation and checks happen here.
	 */
	private static final class StateCreatingFlatMap
		extends RichFlatMapFunction<Long, String> implements CheckpointedFunction, CheckpointListener {

		private static final long serialVersionUID = 1L;

		/**
		 * User configured size of the generated artificial values in the keyed state.
		 */
		final int valueSize;

		/**
		 * Holds the user configuration if the artificial test failure is killing the JVM.
		 */
		final boolean killTaskOnFailure;

		/**
		 * This state is used to create artificial keyed state in the backend.
		 */
		transient ValueState<String> valueState;

		/**
		 * This state is used to persist the schedulingAndFailureInfo to state.
		 */
		transient ListState<MapperSchedulingAndFailureInfo> schedulingAndFailureState;

		/**
		 * This contains the current scheduling and failure meta data.
		 */
		transient MapperSchedulingAndFailureInfo currentSchedulingAndFailureInfo;

		/**
		 * Message to indicate that recovery detected a failure with sticky allocation.
		 */
		transient volatile String allocationFailureMessage;

		/**
		 * If this flag is true, the next invocation of the map function introduces a test failure.
		 */
		transient volatile boolean failTask;

		StateCreatingFlatMap(int valueSize, boolean killTaskOnFailure) {
			this.valueSize = valueSize;
			this.failTask = false;
			this.killTaskOnFailure = killTaskOnFailure;
			this.allocationFailureMessage = null;
		}

		@Override
		public void flatMap(Long key, Collector<String> collector) throws IOException {

			if (allocationFailureMessage != null) {
				// Report the failure downstream, so that we can get the message from the output.
				collector.collect(allocationFailureMessage);
				allocationFailureMessage = null;
			}

			if (failTask) {
				// we fail the task, either by killing the JVM hard, or by throwing a user code exception.
				if (killTaskOnFailure) {
					Runtime.getRuntime().halt(-1);
				} else {
					throw new RuntimeException("Artificial user code exception.");
				}
			}

			// sanity check
			if (null != valueState.value()) {
				throw new IllegalStateException("This should never happen, keys are generated monotonously.");
			}

			// store artificial data to blow up the state
			valueState.update(RandomStringUtils.random(valueSize, true, true));
		}

		@Override
		public void snapshotState(FunctionSnapshotContext functionSnapshotContext) {
		}

		@Override
		public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
			ValueStateDescriptor<String> stateDescriptor =
				new ValueStateDescriptor<>("state", String.class);
			valueState = functionInitializationContext.getKeyedStateStore().getState(stateDescriptor);

			ListStateDescriptor<MapperSchedulingAndFailureInfo> mapperInfoStateDescriptor =
				new ListStateDescriptor<>("mapperState", MapperSchedulingAndFailureInfo.class);
			schedulingAndFailureState =
				functionInitializationContext.getOperatorStateStore().getUnionListState(mapperInfoStateDescriptor);

			StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) getRuntimeContext();
			String allocationID = runtimeContext.getAllocationIDAsString();
			// Pattern of the name: "Flat Map -> Sink: Unnamed (4/4)#0". Remove "#0" part:
			String taskNameWithSubtasks = runtimeContext.getTaskNameWithSubtasks().split("#")[0];

			final int thisJvmPid = getJvmPid();
			final Set<Integer> killedJvmPids = new HashSet<>();

			// here we check if the sticky scheduling worked as expected
			if (functionInitializationContext.isRestored()) {
				Iterable<MapperSchedulingAndFailureInfo> iterable = schedulingAndFailureState.get();

				MapperSchedulingAndFailureInfo infoForThisTask = null;
				List<MapperSchedulingAndFailureInfo> completeInfo = new ArrayList<>();
				if (iterable != null) {
					for (MapperSchedulingAndFailureInfo testInfo : iterable) {

						completeInfo.add(testInfo);

						if (taskNameWithSubtasks.equals(testInfo.taskNameWithSubtask)) {
							infoForThisTask = testInfo;
						}

						if (testInfo.killedJvm) {
							killedJvmPids.add(testInfo.jvmPid);
						}
					}
				}

				Preconditions.checkNotNull(infoForThisTask, "Expected to find info here.");

				if (!isScheduledToCorrectAllocation(infoForThisTask, allocationID, killedJvmPids)) {
					allocationFailureMessage = String.format(
						"Sticky allocation test failed: Subtask %s in attempt %d was rescheduled from allocation %s " +
							"on JVM with PID %d to unexpected allocation %s on JVM with PID %d.\n" +
							"Complete information from before the crash: %s.",
						taskNameWithSubtasks,
						runtimeContext.getAttemptNumber(),
						infoForThisTask.allocationId,
						infoForThisTask.jvmPid,
						allocationID,
						thisJvmPid,
						completeInfo);
				}
			}

			// We determine which of the subtasks will produce the artificial failure
			boolean failingTask = shouldTaskFailForThisAttempt();

			// We take note of all the meta info that we require to check sticky scheduling in the next re-attempt
			this.currentSchedulingAndFailureInfo = new MapperSchedulingAndFailureInfo(
				failingTask,
				failingTask && killTaskOnFailure,
				thisJvmPid,
				taskNameWithSubtasks,
				allocationID);

			schedulingAndFailureState.clear();
			schedulingAndFailureState.add(currentSchedulingAndFailureInfo);
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) {
			// we can only fail the task after at least one checkpoint is completed to record progress.
			failTask = currentSchedulingAndFailureInfo.failingTask;
		}

		@Override
		public void notifyCheckpointAborted(long checkpointId) {
		}

		private boolean shouldTaskFailForThisAttempt() {
			RuntimeContext runtimeContext = getRuntimeContext();
			int numSubtasks = runtimeContext.getNumberOfParallelSubtasks();
			int subtaskIdx = runtimeContext.getIndexOfThisSubtask();
			int attempt = runtimeContext.getAttemptNumber();
			return (attempt % numSubtasks) == subtaskIdx;
		}

		private boolean isScheduledToCorrectAllocation(
			MapperSchedulingAndFailureInfo infoForThisTask,
			String allocationID,
			Set<Integer> killedJvmPids) {

			return (infoForThisTask.allocationId.equals(allocationID)
				|| killedJvmPids.contains(infoForThisTask.jvmPid));
		}
	}

	/**
	 * This code is copied from Stack Overflow.
	 *
	 * <p><a href="https://stackoverflow.com/questions/35842">https://stackoverflow.com/questions/35842</a>, answer
	 * <a href="https://stackoverflow.com/a/12066696/9193881">https://stackoverflow.com/a/12066696/9193881</a>
	 *
	 * <p>Author: <a href="https://stackoverflow.com/users/446591/brad-mace">Brad Mace</a>)
	 */
	private static int getJvmPid() throws Exception {
		java.lang.management.RuntimeMXBean runtime =
			java.lang.management.ManagementFactory.getRuntimeMXBean();
		java.lang.reflect.Field jvm = runtime.getClass().getDeclaredField("jvm");
		jvm.setAccessible(true);
		sun.management.VMManagement mgmt =
			(sun.management.VMManagement) jvm.get(runtime);
		java.lang.reflect.Method pidMethod =
			mgmt.getClass().getDeclaredMethod("getProcessId");
		pidMethod.setAccessible(true);

		return (int) (Integer) pidMethod.invoke(mgmt);
	}

	/**
	 * Records the information required to check sticky scheduling after a restart.
	 */
	public static class MapperSchedulingAndFailureInfo implements Serializable {

		private static final long serialVersionUID = 1L;

		/**
		 * True iff this task inflicts a test failure.
		 */
		final boolean failingTask;

		/**
		 * True iff this task kills its JVM.
		 */
		final boolean killedJvm;

		/**
		 * PID of the task JVM.
		 */
		final int jvmPid;

		/**
		 * Name and subtask index of the task.
		 */
		final String taskNameWithSubtask;

		/**
		 * The current allocation id of this task.
		 */
		final String allocationId;

		MapperSchedulingAndFailureInfo(
			boolean failingTask,
			boolean killedJvm,
			int jvmPid,
			String taskNameWithSubtask,
			String allocationId) {

			this.failingTask = failingTask;
			this.killedJvm = killedJvm;
			this.jvmPid = jvmPid;
			this.taskNameWithSubtask = taskNameWithSubtask;
			this.allocationId = allocationId;
		}

		@Override
		public String toString() {
			return "MapperTestInfo{" +
				"failingTask=" + failingTask +
				", killedJvm=" + killedJvm +
				", jvmPid=" + jvmPid +
				", taskNameWithSubtask='" + taskNameWithSubtask + '\'' +
				", allocationId='" + allocationId + '\'' +
				'}';
		}
	}
}
