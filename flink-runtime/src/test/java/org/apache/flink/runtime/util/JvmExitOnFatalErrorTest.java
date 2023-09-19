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
import org.apache.flink.runtime.blob.VoidPermanentBlobService;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TaskExecutorStateChangelogStoragesManager;
import org.apache.flink.runtime.state.TaskLocalStateStore;
import org.apache.flink.runtime.state.TaskLocalStateStoreImpl;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.inmemory.InMemoryStateChangelogStorage;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.NoOpPartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.TaskExecutorResourceUtils;
import org.apache.flink.runtime.taskexecutor.TaskManagerConfiguration;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.NoOpCheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.runtime.taskmanager.NoOpTaskOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.testutils.TestJvmProcess;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.SerializedValue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.net.InetAddress;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Test that verifies the behavior of blocking shutdown hooks and of the {@link
 * JvmShutdownSafeguard} that guards against it.
 */
class JvmExitOnFatalErrorTest {

    @TempDir private java.nio.file.Path tempDir;

    @Test
    void testExitJvmOnOutOfMemory() throws Exception {
        // this test works only on linux and MacOS
        assumeThat(OperatingSystem.isWindows()).isFalse();

        // to check what went wrong (when the test hangs) uncomment this line
        //        ProcessEntryPoint.main(new
        // String[]{TempDirUtils.newFolder(tempDir).getAbsolutePath()});

        final KillOnFatalErrorProcess testProcess =
                new KillOnFatalErrorProcess(TempDirUtils.newFolder(tempDir));

        try {
            testProcess.startProcess();
            testProcess.waitFor();
        } finally {
            testProcess.destroy();
        }
    }

    // ------------------------------------------------------------------------
    //  Blocking Process Implementation
    // ------------------------------------------------------------------------

    private static final class KillOnFatalErrorProcess extends TestJvmProcess {

        private final File temporaryFolder;

        public KillOnFatalErrorProcess(File temporaryFolder) throws Exception {
            this.temporaryFolder = temporaryFolder;
        }

        @Override
        public String getName() {
            return "KillOnFatalErrorProcess";
        }

        @Override
        public String[] getJvmArgs() {
            return new String[] {temporaryFolder.getAbsolutePath()};
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
                final AllocationID allocationID = new AllocationID();
                final JobVertexID jobVertexId = new JobVertexID();
                final ExecutionAttemptID executionAttemptID = createExecutionAttemptId(jobVertexId);
                final AllocationID slotAllocationId = new AllocationID();

                final SerializedValue<ExecutionConfig> execConfig =
                        new SerializedValue<>(new ExecutionConfig());

                final JobInformation jobInformation =
                        new JobInformation(
                                jid,
                                "Test Job",
                                execConfig,
                                new Configuration(),
                                Collections.emptyList(),
                                Collections.emptyList());

                final TaskInformation taskInformation =
                        new TaskInformation(
                                jobVertexId,
                                "Test Task",
                                1,
                                1,
                                OomInvokable.class.getName(),
                                new Configuration());

                final MemoryManager memoryManager =
                        MemoryManagerBuilder.newBuilder().setMemorySize(1024 * 1024).build();
                final IOManager ioManager = new IOManagerAsync();

                final ShuffleEnvironment<?, ?> shuffleEnvironment =
                        new NettyShuffleEnvironmentBuilder().build();

                final Configuration copiedConf = new Configuration(taskManagerConfig);
                final File tmpWorkingDirectory = new File(args[0]);
                final TaskManagerRuntimeInfo tmInfo =
                        TaskManagerConfiguration.fromConfiguration(
                                taskManagerConfig,
                                TaskExecutorResourceUtils.resourceSpecFromConfigForLocalExecution(
                                        copiedConf),
                                InetAddress.getLoopbackAddress().getHostAddress(),
                                tmpWorkingDirectory);

                final Executor executor = Executors.newCachedThreadPool();

                final TaskLocalStateStore localStateStore =
                        new TaskLocalStateStoreImpl(
                                jid,
                                allocationID,
                                jobVertexId,
                                executionAttemptID.getSubtaskIndex(),
                                TestLocalRecoveryConfig.disabled(),
                                executor);

                final StateChangelogStorage<?> changelogStorage =
                        new InMemoryStateChangelogStorage();

                final TaskStateManager slotStateManager =
                        new TaskStateManagerImpl(
                                jid,
                                executionAttemptID,
                                localStateStore,
                                null,
                                changelogStorage,
                                new TaskExecutorStateChangelogStoragesManager(),
                                null,
                                NoOpCheckpointResponder.INSTANCE);

                Task task =
                        new Task(
                                jobInformation,
                                taskInformation,
                                executionAttemptID,
                                slotAllocationId,
                                Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
                                Collections.<InputGateDeploymentDescriptor>emptyList(),
                                memoryManager,
                                new SharedResources(),
                                ioManager,
                                shuffleEnvironment,
                                new KvStateService(new KvStateRegistry(), null, null),
                                new BroadcastVariableManager(),
                                new TaskEventDispatcher(),
                                ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
                                slotStateManager,
                                new NoOpTaskManagerActions(),
                                new NoOpInputSplitProvider(),
                                NoOpCheckpointResponder.INSTANCE,
                                new NoOpTaskOperatorEventGateway(),
                                new TestGlobalAggregateManager(),
                                TestingClassLoaderLease.newBuilder().build(),
                                new FileCache(
                                        tmInfo.getTmpDirectories(),
                                        VoidPermanentBlobService.INSTANCE),
                                tmInfo,
                                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
                                new NoOpPartitionProducerStateChecker(),
                                executor,
                                new ChannelStateWriteRequestExecutorFactory(
                                        jobInformation.getJobId()));

                System.err.println("starting task thread");

                task.startTaskThread();
            } catch (Throwable t) {
                System.err.println("ERROR STARTING TASK");
                t.printStackTrace();
            }

            System.err.println("parking the main thread");
            CommonTestUtils.blockForeverNonInterruptibly();
        }

        public static final class OomInvokable extends AbstractInvokable {

            public OomInvokable(Environment environment) {
                super(environment);
            }

            @Override
            public void invoke() throws Exception {
                throw new OutOfMemoryError();
            }
        }

        private static final class NoOpInputSplitProvider implements InputSplitProvider {

            @Override
            public InputSplit getNextInputSplit(ClassLoader userCodeClassLoader) {
                return null;
            }
        }
    }
}
