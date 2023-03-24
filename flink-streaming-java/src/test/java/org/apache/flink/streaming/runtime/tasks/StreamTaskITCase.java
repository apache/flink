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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.ExpectedTestException;
import org.apache.flink.runtime.shuffle.PartitionDescriptorBuilder;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TestTaskBuilder;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.FunctionWithException;

import org.hamcrest.CoreMatchers;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;

import static java.util.Collections.singletonList;
import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_PERIOD;
import static org.apache.flink.runtime.io.network.api.writer.RecordWriter.DEFAULT_OUTPUT_FLUSH_THREAD_NAME;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/** Tests for {@link StreamTask}. */
public class StreamTaskITCase extends TestLogger {
    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    @Test
    public void testRecordWriterClosedOnTransitDeployingStateError() throws Exception {
        testRecordWriterClosedOnTransitStateError(ExecutionState.DEPLOYING);
    }

    @Test
    public void testRecordWriterClosedOnTransitInitializingStateError() throws Exception {
        testRecordWriterClosedOnTransitStateError(ExecutionState.INITIALIZING);
    }

    @Test
    public void testRecordWriterClosedOnTransitRunningStateError() throws Exception {
        testRecordWriterClosedOnTransitStateError(ExecutionState.RUNNING);
    }

    private void testRecordWriterClosedOnTransitStateError(ExecutionState executionState)
            throws Exception {
        // Throw the exception when the state updating to the expected one.
        NoOpTaskManagerActions taskManagerActions =
                new NoOpTaskManagerActions() {
                    @Override
                    public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
                        if (taskExecutionState.getExecutionState() == executionState) {
                            throw new ExpectedTestException();
                        }
                    }
                };

        testRecordWriterClosedOnError(
                env ->
                        taskBuilderWithConfiguredRecordWriter(env)
                                .setTaskManagerActions(taskManagerActions)
                                .build(EXECUTOR_RESOURCE.getExecutor()));
    }

    @Test
    public void testFailInEndOfConstructor() throws Exception {
        Configuration conf = new Configuration();
        // Set the wrong setting type for forcing the fail during read.
        conf.setString(BUFFER_DEBLOAT_PERIOD.key(), "a");
        testRecordWriterClosedOnError(
                env ->
                        taskBuilderWithConfiguredRecordWriter(env)
                                .setTaskManagerConfig(conf)
                                .build(EXECUTOR_RESOURCE.getExecutor()));
    }

    private void testRecordWriterClosedOnError(
            FunctionWithException<NettyShuffleEnvironment, Task, Exception> taskProvider)
            throws Exception {
        try (NettyShuffleEnvironment shuffleEnvironment =
                new NettyShuffleEnvironmentBuilder().build()) {
            Task task = taskProvider.apply(shuffleEnvironment);

            task.startTaskThread();
            task.getExecutingThread().join();

            assertEquals(ExecutionState.FAILED, task.getExecutionState());
            for (Thread thread : Thread.getAllStackTraces().keySet()) {
                assertThat(
                        thread.getName(),
                        CoreMatchers.is(not(containsString(DEFAULT_OUTPUT_FLUSH_THREAD_NAME))));
            }
        }
    }

    private TestTaskBuilder taskBuilderWithConfiguredRecordWriter(
            NettyShuffleEnvironment shuffleEnvironment) {
        Configuration taskConfiguration = new Configuration();
        outputEdgeConfiguration(taskConfiguration);

        ResultPartitionDeploymentDescriptor descriptor =
                new ResultPartitionDeploymentDescriptor(
                        PartitionDescriptorBuilder.newBuilder().build(),
                        NettyShuffleDescriptorBuilder.newBuilder().buildLocal(),
                        1);
        return new TestTaskBuilder(shuffleEnvironment)
                .setInvokable(NoOpStreamTask.class)
                .setTaskConfig(taskConfiguration)
                .setResultPartitions(singletonList(descriptor));
    }

    /**
     * Make sure that there is some output edge in the config so that some RecordWriter is created.
     */
    private void outputEdgeConfiguration(Configuration taskConfiguration) {
        StreamConfig streamConfig = new StreamConfig(taskConfiguration);
        streamConfig.setStreamOperatorFactory(new UnusedOperatorFactory());

        StreamConfigChainer cfg =
                new StreamConfigChainer(new OperatorID(42, 42), streamConfig, this, 1);
        // The OutputFlusher thread is started only if the buffer timeout more than 0(default value
        // is 0).
        cfg.setBufferTimeout(1);
        cfg.chain(
                new OperatorID(44, 44),
                new UnusedOperatorFactory(),
                StringSerializer.INSTANCE,
                StringSerializer.INSTANCE,
                false);
        cfg.finish();
    }

    // ------------------------------------------------------------------------
    //  Test Utilities
    // ------------------------------------------------------------------------

    /**
     * Operator that does nothing.
     *
     * @param <T>
     * @param <OP>
     */
    public static class NoOpStreamTask<T, OP extends StreamOperator<T>> extends StreamTask<T, OP> {

        public NoOpStreamTask(Environment environment) throws Exception {
            super(environment);
        }

        @Override
        protected void init() throws Exception {
            inputProcessor = new StreamTaskTest.EmptyInputProcessor();
        }

        @Override
        protected void cleanUpInternal() throws Exception {}
    }

    // ------------------------------------------------------------------------
    // ------------------------------------------------------------------------

    private static class UnusedOperatorFactory extends AbstractStreamOperatorFactory<String> {
        @Override
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            throw new UnsupportedOperationException("This shouldn't be called");
        }

        @Override
        public void setChainingStrategy(ChainingStrategy strategy) {}

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            throw new UnsupportedOperationException();
        }
    }
}
