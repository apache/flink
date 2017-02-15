/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.FallbackLibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.testutils.UnregisteredTaskMetricsGroup;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskActions;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.testutils.TestJvmProcess;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.SerializedValue;

import org.junit.Test;

import java.net.URL;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.*;

/**
 * Test that verifies the behavior of blocking shutdown hooks and of the
 * {@link JvmShutdownSafeguard} that guards against it.
 */
public class JvmExitOnFatalErrorTest {

	@Test
	public void testExitJvmOnOutOfMemory() throws Exception {
		// this test works only on linux
		assumeTrue(OperatingSystem.isLinux());

		// this test leaves remaining processes if not executed with Java 8
		CommonTestUtils.assumeJava8();

		// to check what went wrong (when the test hangs) uncomment this line 
//		ProcessEntryPoint.main(new String[0]);

		final KillOnFatalErrorProcess testProcess = new KillOnFatalErrorProcess();

		try {
			testProcess.startProcess();
			testProcess.waitFor();
		}
		finally {
			testProcess.destroy();
		}
	}

	// ------------------------------------------------------------------------
	//  Blocking Process Implementation
	// ------------------------------------------------------------------------

	private static final class KillOnFatalErrorProcess extends TestJvmProcess {

		public KillOnFatalErrorProcess() throws Exception {}

		@Override
		public String getName() {
			return "KillOnFatalErrorProcess";
		}

		@Override
		public String[] getJvmArgs() {
			return new String[0];
		}

		@Override
		public String getEntryPointClassName() {
			return ProcessEntryPoint.class.getName();
		}
	} 

	// ------------------------------------------------------------------------

	public static final class ProcessEntryPoint {

		public static void main(String[] args) throws Exception {

			System.err.println("creating task");

			// we suppress process exits via errors here to not
			// have a test that exits accidentally due to a programming error 
			try {
				final Configuration taskManagerConfig = new Configuration();
				taskManagerConfig.setBoolean(TaskManagerOptions.KILL_ON_OUT_OF_MEMORY, true);

				final JobID jid = new JobID();
				final JobVertexID jobVertexId = new JobVertexID();
				final ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();
				final AllocationID slotAllocationId = new AllocationID();

				final SerializedValue<ExecutionConfig> execConfig = new SerializedValue<>(new ExecutionConfig());

				final JobInformation jobInformation = new JobInformation(
						jid, "Test Job", execConfig, new Configuration(),
						Collections.<BlobKey>emptyList(), Collections.<URL>emptyList());

				final TaskInformation taskInformation = new TaskInformation(
						jobVertexId, "Test Task", 1, 1, OomInvokable.class.getName(), new Configuration());

				final MemoryManager memoryManager = new MemoryManager(1024 * 1024, 1);
				final IOManager ioManager = new IOManagerAsync();

				final NetworkEnvironment networkEnvironment = mock(NetworkEnvironment.class);
				when(networkEnvironment.createKvStateTaskRegistry(jid, jobVertexId)).thenReturn(mock(TaskKvStateRegistry.class));

				final TaskManagerRuntimeInfo tmInfo = TaskManagerConfiguration.fromConfiguration(taskManagerConfig);

				final Executor executor = Executors.newCachedThreadPool();

				Task task = new Task(
						jobInformation,
						taskInformation,
						executionAttemptID,
						slotAllocationId,
						0,       // subtaskIndex
						0,       // attemptNumber
						Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
						Collections.<InputGateDeploymentDescriptor>emptyList(),
						0,       // targetSlotNumber
						null,    // taskStateHandles,
						memoryManager,
						ioManager,
						networkEnvironment,
						new BroadcastVariableManager(),
						new NoOpTaskManagerActions(),
						new NoOpInputSplitProvider(),
						new NoOpCheckpointResponder(),
						new FallbackLibraryCacheManager(),
						new FileCache(tmInfo.getTmpDirectories()),
						tmInfo,
						new UnregisteredTaskMetricsGroup(),
						new NoOpResultPartitionConsumableNotifier(),
						new NoOpPartitionProducerStateChecker(),
						executor);

				System.err.println("starting task thread");

				task.startTaskThread();
			}
			catch (Throwable t) {
				System.err.println("ERROR STARTING TASK");
				t.printStackTrace();
			}

			System.err.println("parking the main thread");
			CommonTestUtils.blockForeverNonInterruptibly();
		}

		public static final class OomInvokable extends AbstractInvokable {

			@Override
			public void invoke() throws Exception {
				throw new OutOfMemoryError();
			}
		}

		private static final class NoOpTaskManagerActions implements TaskManagerActions {

			@Override
			public void notifyFinalState(ExecutionAttemptID executionAttemptID) {}

			@Override
			public void notifyFatalError(String message, Throwable cause) {}

			@Override
			public void failTask(ExecutionAttemptID executionAttemptID, Throwable cause) {}

			@Override
			public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {}
		}

		private static final class NoOpInputSplitProvider implements InputSplitProvider {

			@Override
			public InputSplit getNextInputSplit(ClassLoader userCodeClassLoader) {
				return null;
			}
		}

		private static final class NoOpCheckpointResponder implements CheckpointResponder {

			@Override
			public void acknowledgeCheckpoint(JobID j, ExecutionAttemptID e, CheckpointMetaData c, SubtaskState s) {}

			@Override
			public void declineCheckpoint(JobID j, ExecutionAttemptID e, long l, Throwable t) {}
		}

		private static final class NoOpResultPartitionConsumableNotifier implements ResultPartitionConsumableNotifier {

			@Override
			public void notifyPartitionConsumable(JobID j, ResultPartitionID p, TaskActions t) {}
		}

		private static final class NoOpPartitionProducerStateChecker implements PartitionProducerStateChecker {

			@Override
			public Future<ExecutionState> requestPartitionProducerState(
					JobID jobId, IntermediateDataSetID intermediateDataSetId, ResultPartitionID r) {
				return null;
			}
		}
	}
}
