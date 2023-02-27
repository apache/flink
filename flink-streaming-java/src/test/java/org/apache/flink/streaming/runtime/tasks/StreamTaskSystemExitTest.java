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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.core.security.UserSystemExitException;
import org.apache.flink.runtime.broadcast.BroadcastVariableManager;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriteRequestExecutorFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.librarycache.TestingClassLoaderLease;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.disk.iomanager.IOManagerAsync;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.memory.MemoryManagerBuilder;
import org.apache.flink.runtime.memory.SharedResources;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.TestGlobalAggregateManager;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpTaskOperatorEventGateway;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;

/**
 * Tests for stream task where user-invokable codes try to exit JVM. Currently, monitoring system
 * exit is enabled inside relevant methods that can call user-defined functions in {@code
 * StreamTask}.
 */
public class StreamTaskSystemExitTest extends TestLogger {
    private static final int TEST_EXIT_CODE = 123;
    private SecurityManager originalSecurityManager;

    /**
     * Perform the check System.exit() does through security manager without actually calling
     * System.exit() not to confuse Junit about failed test.
     */
    private static void systemExit() {
        SecurityManager securityManager = System.getSecurityManager();
        if (securityManager != null) {
            securityManager.checkExit(TEST_EXIT_CODE);
        }
    }

    @Before
    public void setUp() {
        Configuration configuration = new Configuration();
        configuration.set(
                ClusterOptions.INTERCEPT_USER_SYSTEM_EXIT, ClusterOptions.UserSystemExitMode.THROW);
        originalSecurityManager = System.getSecurityManager();
        FlinkSecurityManager.setFromConfiguration(configuration);
    }

    @After
    public void tearDown() {
        System.setSecurityManager(originalSecurityManager);
    }

    @Test
    public void testInitSystemExitStreamTask() throws Exception {
        Task task = createSystemExitTask(InitSystemExitStreamTask.class.getName(), null);
        task.run();
        assertNotNull(task.getFailureCause());
        assertEquals(task.getFailureCause().getClass(), UserSystemExitException.class);
    }

    @Test
    public void testProcessInputSystemExitStreamTask() throws Exception {
        Task task = createSystemExitTask(ProcessInputSystemExitStreamTask.class.getName(), null);
        task.run();
        assertNotNull(task.getFailureCause());
        assertEquals(task.getFailureCause().getClass(), UserSystemExitException.class);
    }

    @Test(expected = UserSystemExitException.class)
    public void testCancelSystemExitStreamTask() throws Exception {
        Environment mockEnvironment = new MockEnvironmentBuilder().build();
        SystemExitStreamTask systemExitStreamTask =
                new SystemExitStreamTask(mockEnvironment, SystemExitStreamTask.ExitPoint.CANCEL);
        systemExitStreamTask.cancel();
    }

    @Test
    public void testStreamSourceSystemExitStreamTask() throws Exception {
        final TestStreamSource<String, SystemExitSourceFunction> testStreamSource =
                new TestStreamSource<>(new SystemExitSourceFunction());
        Task task =
                createSystemExitTask(SystemExitSourceStreamTask.class.getName(), testStreamSource);
        task.run();
        assertNotNull(task.getFailureCause());
        assertEquals(task.getFailureCause().getClass(), UserSystemExitException.class);
    }

    private Task createSystemExitTask(final String invokableClassName, StreamOperator<?> operator)
            throws Exception {
        final Configuration taskConfiguration = new Configuration();
        final StreamConfig streamConfig = new StreamConfig(taskConfiguration);
        streamConfig.setOperatorID(new OperatorID());
        streamConfig.setStreamOperator(operator);
        streamConfig.setTimeCharacteristic(TimeCharacteristic.ProcessingTime); // for source run
        streamConfig.serializeAllConfigs();

        final JobInformation jobInformation =
                new JobInformation(
                        new JobID(),
                        "Test Job",
                        new SerializedValue<>(new ExecutionConfig()),
                        new Configuration(),
                        Collections.emptyList(),
                        Collections.emptyList());

        final TaskInformation taskInformation =
                new TaskInformation(
                        new JobVertexID(),
                        "Test Task",
                        1,
                        1,
                        invokableClassName,
                        taskConfiguration);

        final TaskManagerRuntimeInfo taskManagerRuntimeInfo = new TestingTaskManagerRuntimeInfo();

        final ShuffleEnvironment<?, ?> shuffleEnvironment =
                new NettyShuffleEnvironmentBuilder().build();

        return new Task(
                jobInformation,
                taskInformation,
                createExecutionAttemptId(taskInformation.getJobVertexId()),
                new AllocationID(),
                Collections.<ResultPartitionDeploymentDescriptor>emptyList(),
                Collections.<InputGateDeploymentDescriptor>emptyList(),
                MemoryManagerBuilder.newBuilder().setMemorySize(32L * 1024L).build(),
                new SharedResources(),
                new IOManagerAsync(),
                shuffleEnvironment,
                new KvStateService(new KvStateRegistry(), null, null),
                mock(BroadcastVariableManager.class),
                new TaskEventDispatcher(),
                ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES,
                new TestTaskStateManager(),
                mock(TaskManagerActions.class),
                mock(InputSplitProvider.class),
                mock(CheckpointResponder.class),
                new NoOpTaskOperatorEventGateway(),
                new TestGlobalAggregateManager(),
                TestingClassLoaderLease.newBuilder().build(),
                mock(FileCache.class),
                taskManagerRuntimeInfo,
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup(),
                mock(PartitionProducerStateChecker.class),
                Executors.directExecutor(),
                new ChannelStateWriteRequestExecutorFactory(jobInformation.getJobId()));
    }

    /** StreamTask emulating system exit behavior from different callback functions. */
    public static class SystemExitStreamTask
            extends StreamTask<String, AbstractStreamOperator<String>> {
        private final ExitPoint exitPoint;

        public SystemExitStreamTask(Environment env) throws Exception {
            this(env, ExitPoint.NONE);
        }

        public SystemExitStreamTask(Environment env, ExitPoint exitPoint) throws Exception {
            super(env, null);
            this.exitPoint = exitPoint;
        }

        @Override
        protected void init() {
            if (exitPoint == ExitPoint.INIT) {
                systemExit();
            }
        }

        @Override
        protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
            if (exitPoint == ExitPoint.PROCESS_INPUT) {
                systemExit();
            }
        }

        @Override
        protected void cleanUpInternal() {}

        @Override
        protected void cancelTask() {
            if (exitPoint == ExitPoint.CANCEL) {
                systemExit();
            }
        }

        /** Inside invoke() call, specify where system exit is called. */
        protected enum ExitPoint {
            NONE,
            INIT,
            PROCESS_INPUT,
            CANCEL,
        }
    }

    /**
     * {@link SystemExitStreamTask} with default constructor configured to exit at init function.
     */
    public static class InitSystemExitStreamTask extends SystemExitStreamTask {
        public InitSystemExitStreamTask(Environment env) throws Exception {
            super(env, ExitPoint.INIT);
        }
    }

    /**
     * {@link SystemExitStreamTask} with default constructor configured to exit at process input
     * function.
     */
    public static class ProcessInputSystemExitStreamTask extends SystemExitStreamTask {
        public ProcessInputSystemExitStreamTask(Environment env) throws Exception {
            super(env, ExitPoint.PROCESS_INPUT);
        }
    }

    /**
     * SourceStreamTask emulating system exit behavior in run function by {@link
     * SystemExitSourceFunction}.
     */
    public static class SystemExitSourceStreamTask
            extends SourceStreamTask<
                    String,
                    SystemExitSourceFunction,
                    StreamTaskTest.TestStreamSource<String, SystemExitSourceFunction>> {

        public SystemExitSourceStreamTask(Environment env) throws Exception {
            super(env);
        }
    }

    private static class SystemExitSourceFunction implements SourceFunction<String> {

        @Override
        public void run(SourceContext<String> ctx) {
            systemExit();
        }

        @Override
        public void cancel() {}
    }

    static class TestStreamSource<OUT, SRC extends SourceFunction<OUT>>
            extends StreamSource<OUT, SRC> {

        public TestStreamSource(SRC sourceFunction) {
            super(sourceFunction);
        }
    }
}
