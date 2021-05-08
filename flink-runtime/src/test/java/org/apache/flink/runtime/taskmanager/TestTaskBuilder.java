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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.partition.NoOpResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.NoOpPartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import static org.mockito.Mockito.mock;

/** Util that helps building {@link Task} objects for testing. */
public final class TestTaskBuilder {

    private Class<? extends AbstractInvokable> invokable = AbstractInvokable.class;
    private TaskManagerActions taskManagerActions = new NoOpTaskManagerActions();
    private LibraryCacheManager.ClassLoaderHandle classLoaderHandle =
            TestingClassLoaderLease.newBuilder().build();
    private ResultPartitionConsumableNotifier consumableNotifier =
            new NoOpResultPartitionConsumableNotifier();
    private PartitionProducerStateChecker partitionProducerStateChecker =
            new NoOpPartitionProducerStateChecker();
    private final ShuffleEnvironment<?, ?> shuffleEnvironment;
    private KvStateService kvStateService = new KvStateService(new KvStateRegistry(), null, null);
    private Executor executor = TestingUtils.defaultExecutor();
    private Configuration taskManagerConfig = new Configuration();
    private Configuration taskConfig = new Configuration();
    private ExecutionConfig executionConfig = new ExecutionConfig();
    private Collection<PermanentBlobKey> requiredJarFileBlobKeys = Collections.emptyList();
    private List<ResultPartitionDeploymentDescriptor> resultPartitions = Collections.emptyList();
    private List<InputGateDeploymentDescriptor> inputGates = Collections.emptyList();
    private JobID jobId = new JobID();
    private AllocationID allocationID = new AllocationID();
    private ExecutionAttemptID executionAttemptId = new ExecutionAttemptID();
    private ExternalResourceInfoProvider externalResourceInfoProvider =
            ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES;

    public TestTaskBuilder(ShuffleEnvironment<?, ?> shuffleEnvironment) {
        this.shuffleEnvironment = Preconditions.checkNotNull(shuffleEnvironment);
    }

    public TestTaskBuilder setInvokable(Class<? extends AbstractInvokable> invokable) {
        this.invokable = invokable;
        return this;
    }

    public TestTaskBuilder setTaskManagerActions(TaskManagerActions taskManagerActions) {
        this.taskManagerActions = taskManagerActions;
        return this;
    }

    public TestTaskBuilder setClassLoaderHandle(
            LibraryCacheManager.ClassLoaderHandle classLoaderHandle) {
        this.classLoaderHandle = classLoaderHandle;
        return this;
    }

    public TestTaskBuilder setConsumableNotifier(
            ResultPartitionConsumableNotifier consumableNotifier) {
        this.consumableNotifier = consumableNotifier;
        return this;
    }

    public TestTaskBuilder setPartitionProducerStateChecker(
            PartitionProducerStateChecker partitionProducerStateChecker) {
        this.partitionProducerStateChecker = partitionProducerStateChecker;
        return this;
    }

    public TestTaskBuilder setKvStateService(KvStateService kvStateService) {
        this.kvStateService = kvStateService;
        return this;
    }

    public TestTaskBuilder setExecutor(Executor executor) {
        this.executor = executor;
        return this;
    }

    public TestTaskBuilder setTaskManagerConfig(Configuration taskManagerConfig) {
        this.taskManagerConfig = taskManagerConfig;
        return this;
    }

    public TestTaskBuilder setTaskConfig(Configuration taskConfig) {
        this.taskConfig = taskConfig;
        return this;
    }

    public TestTaskBuilder setExecutionConfig(ExecutionConfig executionConfig) {
        this.executionConfig = executionConfig;
        return this;
    }

    public TestTaskBuilder setRequiredJarFileBlobKeys(
            Collection<PermanentBlobKey> requiredJarFileBlobKeys) {
        this.requiredJarFileBlobKeys = requiredJarFileBlobKeys;
        return this;
    }

    public TestTaskBuilder setResultPartitions(
            List<ResultPartitionDeploymentDescriptor> resultPartitions) {
        this.resultPartitions = resultPartitions;
        return this;
    }

    public TestTaskBuilder setInputGates(List<InputGateDeploymentDescriptor> inputGates) {
        this.inputGates = inputGates;
        return this;
    }

    public TestTaskBuilder setJobId(JobID jobId) {
        this.jobId = jobId;
        return this;
    }

    public TestTaskBuilder setAllocationID(AllocationID allocationID) {
        this.allocationID = allocationID;
        return this;
    }

    public TestTaskBuilder setExecutionAttemptId(ExecutionAttemptID executionAttemptId) {
        this.executionAttemptId = executionAttemptId;
        return this;
    }

    public TestTaskBuilder setExternalResourceInfoProvider(
            ExternalResourceInfoProvider externalResourceInfoProvider) {
        this.externalResourceInfoProvider = externalResourceInfoProvider;
        return this;
    }

    public Task build() throws Exception {
        final JobVertexID jobVertexId = new JobVertexID();

        final SerializedValue<ExecutionConfig> serializedExecutionConfig =
                new SerializedValue<>(executionConfig);

        final JobInformation jobInformation =
                new JobInformation(
                        jobId,
                        "Test Job",
                        serializedExecutionConfig,
                        new Configuration(),
                        requiredJarFileBlobKeys,
                        Collections.emptyList());

        final TaskInformation taskInformation =
                new TaskInformation(
                        jobVertexId, "Test Task", 1, 1, invokable.getName(), taskConfig);

        final TaskMetricGroup taskMetricGroup =
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();

        return new Task(
                jobInformation,
                taskInformation,
                executionAttemptId,
                allocationID,
                0,
                0,
                resultPartitions,
                inputGates,
                MemoryManagerBuilder.newBuilder().setMemorySize(1024 * 1024).build(),
                mock(IOManager.class),
                shuffleEnvironment,
                kvStateService,
                new BroadcastVariableManager(),
                new TaskEventDispatcher(),
                externalResourceInfoProvider,
                new TestTaskStateManager(),
                taskManagerActions,
                new MockInputSplitProvider(),
                new TestCheckpointResponder(),
                new NoOpTaskOperatorEventGateway(),
                new TestGlobalAggregateManager(),
                classLoaderHandle,
                mock(FileCache.class),
                new TestingTaskManagerRuntimeInfo(taskManagerConfig),
                taskMetricGroup,
                consumableNotifier,
                partitionProducerStateChecker,
                executor);
    }

    public static void setTaskState(Task task, ExecutionState state) {
        try {
            Field f = Task.class.getDeclaredField("executionState");
            f.setAccessible(true);
            f.set(task, state);
        } catch (Exception e) {
            throw new RuntimeException("Modifying the task state failed", e);
        }
    }
}
