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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.StreamTestSingleInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProviderImpl;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskmanager.TaskManagerRuntimeInfo;
import org.apache.flink.runtime.util.TestingTaskManagerRuntimeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.NonChainedOutput;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Test harness for testing a {@link StreamTask}.
 *
 * <p>This mock Invokable provides the task with a basic runtime context and allows pushing elements
 * and watermarks into the task. {@link #getOutput()} can be used to get the emitted elements and
 * events. You are free to modify the retrieved list.
 *
 * <p>After setting up everything the Task can be invoked using {@link #invoke()}. This will start a
 * new Thread to execute the Task. Use {@link #waitForTaskCompletion()} to wait for the Task thread
 * to finish.
 *
 * @deprecated Please use {@link StreamTaskMailboxTestHarness} and {@link
 *     StreamTaskMailboxTestHarnessBuilder}. Do not add new code using this test harness.
 */
@Deprecated
public class StreamTaskTestHarness<OUT> {

    public static final int DEFAULT_MEMORY_MANAGER_SIZE = 1024 * 1024;

    public static final int DEFAULT_NETWORK_BUFFER_SIZE = 1024;

    private final FunctionWithException<Environment, ? extends StreamTask<OUT, ?>, Exception>
            taskFactory;

    public long memorySize;
    public int bufferSize;

    protected StreamMockEnvironment mockEnv;
    protected ExecutionConfig executionConfig;
    public Configuration jobConfig;
    public Configuration taskConfig;
    protected StreamConfig streamConfig;
    protected TaskManagerRuntimeInfo taskManagerRuntimeInfo = new TestingTaskManagerRuntimeInfo();

    protected TestTaskStateManager taskStateManager;

    private TypeSerializer<OUT> outputSerializer;
    private TypeSerializer<StreamElement> outputStreamRecordSerializer;

    private LinkedBlockingQueue<Object> outputList;

    protected TaskThread taskThread;

    // These don't get initialized, the one-input/two-input specific test harnesses
    // must initialize these if they want to simulate input. We have them here so that all the
    // input related methods only need to be implemented once, in generic form
    protected int numInputGates;
    protected int numInputChannelsPerGate;

    private boolean setupCalled = false;

    @SuppressWarnings("rawtypes")
    protected StreamTestSingleInputGate[] inputGates;

    public StreamTaskTestHarness(
            FunctionWithException<Environment, ? extends StreamTask<OUT, ?>, Exception> taskFactory,
            TypeInformation<OUT> outputType) {
        this(taskFactory, outputType, TestLocalRecoveryConfig.disabled());
    }

    public StreamTaskTestHarness(
            FunctionWithException<Environment, ? extends StreamTask<OUT, ?>, Exception> taskFactory,
            TypeInformation<OUT> outputType,
            File localRootDir) {
        this(
                taskFactory,
                outputType,
                new LocalRecoveryConfig(
                        new LocalRecoveryDirectoryProviderImpl(
                                localRootDir, new JobID(), new JobVertexID(), 0)));
    }

    public StreamTaskTestHarness(
            FunctionWithException<Environment, ? extends StreamTask<OUT, ?>, Exception> taskFactory,
            TypeInformation<OUT> outputType,
            LocalRecoveryConfig localRecoveryConfig) {
        this.taskFactory = checkNotNull(taskFactory);
        this.memorySize = DEFAULT_MEMORY_MANAGER_SIZE;
        this.bufferSize = DEFAULT_NETWORK_BUFFER_SIZE;

        this.jobConfig = new Configuration();
        this.taskConfig = new Configuration();
        this.executionConfig = new ExecutionConfig();

        streamConfig = new StreamConfig(taskConfig);
        streamConfig.setStateBackendUsesManagedMemory(true);
        streamConfig.setManagedMemoryFractionOperatorOfUseCase(
                ManagedMemoryUseCase.STATE_BACKEND, 1.0);

        outputSerializer = outputType.createSerializer(executionConfig);
        outputStreamRecordSerializer = new StreamElementSerializer<>(outputSerializer);

        this.taskStateManager = new TestTaskStateManager(localRecoveryConfig);
    }

    public StreamMockEnvironment getEnvironment() {
        return mockEnv;
    }

    public TimerService getTimerService() {
        return taskThread.task.getTimerService();
    }

    public TaskManagerRuntimeInfo getTaskManagerRuntimeInfo() {
        return taskManagerRuntimeInfo;
    }

    @SuppressWarnings("unchecked")
    public <OP extends StreamOperator<OUT>> OP getHeadOperator() {
        return (OP) taskThread.task.getMainOperator();
    }

    /** This must be overwritten for OneInputStreamTask or TwoInputStreamTask test harnesses. */
    protected void initializeInputs() throws IOException, InterruptedException {}

    public TestTaskStateManager getTaskStateManager() {
        return taskStateManager;
    }

    public void setTaskStateSnapshot(long checkpointId, TaskStateSnapshot taskStateSnapshot) {
        taskStateManager.setReportedCheckpointId(checkpointId);
        taskStateManager.setJobManagerTaskStateSnapshotsByCheckpointId(
                Collections.singletonMap(checkpointId, taskStateSnapshot));
    }

    private void initializeOutput() {
        outputList = new LinkedBlockingQueue<>();
        mockEnv.addOutput(outputList, outputStreamRecordSerializer);
    }

    /**
     * Users of the test harness can call this utility method to setup the stream config if there
     * will only be a single operator to be tested. The method will setup the outgoing network
     * connection for the operator.
     *
     * <p>For more advanced test cases such as testing chains of multiple operators with the
     * harness, please manually configure the stream config.
     */
    public void setupOutputForSingletonOperatorChain() {
        Preconditions.checkState(!setupCalled, "This harness was already setup.");
        setupCalled = true;
        streamConfig.setChainStart();
        streamConfig.setTimeCharacteristic(TimeCharacteristic.EventTime);
        streamConfig.setNumberOfOutputs(1);
        streamConfig.setTypeSerializerOut(outputSerializer);
        streamConfig.setVertexID(0);
        streamConfig.setOperatorID(new OperatorID(4711L, 123L));

        StreamOperator<OUT> dummyOperator =
                new AbstractStreamOperator<OUT>() {
                    private static final long serialVersionUID = 1L;
                };

        List<NonChainedOutput> streamOutputs = new LinkedList<>();
        StreamNode sourceVertexDummy =
                new StreamNode(
                        0, "group", null, dummyOperator, "source dummy", SourceStreamTask.class);

        streamOutputs.add(
                new NonChainedOutput(
                        true,
                        sourceVertexDummy.getId(),
                        1,
                        1,
                        100,
                        false,
                        new IntermediateDataSetID(),
                        null,
                        new BroadcastPartitioner<>(),
                        ResultPartitionType.PIPELINED_BOUNDED));

        streamConfig.setVertexNonChainedOutputs(streamOutputs);
        streamConfig.setOperatorNonChainedOutputs(streamOutputs);
        streamConfig.serializeAllConfigs();
    }

    public StreamMockEnvironment createEnvironment() {
        StreamMockEnvironment streamMockEnvironment =
                new StreamMockEnvironment(
                        jobConfig,
                        taskConfig,
                        executionConfig,
                        memorySize,
                        new MockInputSplitProvider(),
                        bufferSize,
                        taskStateManager);
        if (taskManagerRuntimeInfo != null) {
            streamMockEnvironment.setTaskManagerInfo(taskManagerRuntimeInfo);
        }

        return streamMockEnvironment;
    }

    /**
     * Invoke the Task. This resets the output of any previous invocation. This will start a new
     * Thread to execute the Task in. Use {@link #waitForTaskCompletion()} to wait for the Task
     * thread to finish running.
     */
    public Thread invoke() throws Exception {
        streamConfig.serializeAllConfigs();
        return invoke(createEnvironment());
    }

    /**
     * Invoke the Task. This resets the output of any previous invocation. This will start a new
     * Thread to execute the Task in. Use {@link #waitForTaskCompletion()} to wait for the Task
     * thread to finish running.
     */
    public Thread invoke(StreamMockEnvironment mockEnv) throws Exception {
        checkState(this.mockEnv == null);
        checkState(this.taskThread == null);
        this.mockEnv = checkNotNull(mockEnv);

        initializeInputs();
        initializeOutput();
        streamConfig.serializeAllConfigs();

        taskThread = new TaskThread(() -> taskFactory.apply(mockEnv));
        taskThread.start();
        // Wait until the task is set
        while (taskThread.task == null) {
            if (taskThread.error != null) {
                ExceptionUtils.rethrow(taskThread.error);
            }
            Thread.sleep(10L);
        }

        return taskThread;
    }

    /** Waits for the task completion. */
    public void waitForTaskCompletion() throws Exception {
        waitForTaskCompletion(Long.MAX_VALUE);
    }

    public void waitForTaskCompletion(long timeout) throws Exception {
        waitForTaskCompletion(timeout, false);
    }

    /**
     * Waits for the task completion. If this does not happen within the timeout, then a
     * TimeoutException is thrown.
     *
     * @param timeout Timeout for the task completion
     */
    public void waitForTaskCompletion(
            long timeout, boolean ignoreCancellationOrInterruptedException) throws Exception {
        Preconditions.checkState(taskThread != null, "Task thread was not started.");

        taskThread.join(timeout);
        if (taskThread.getError() != null) {
            boolean errorIsCancellationOrInterrupted =
                    ExceptionUtils.findThrowable(taskThread.getError(), CancelTaskException.class)
                                    .isPresent()
                            || ExceptionUtils.findThrowable(
                                            taskThread.getError(), InterruptedException.class)
                                    .isPresent();
            if (ignoreCancellationOrInterruptedException && errorIsCancellationOrInterrupted) {
                return;
            }
            throw new Exception("error in task", taskThread.getError());
        }
    }

    /** Waits for the task to be running. */
    public void waitForTaskRunning() throws Exception {
        Preconditions.checkState(taskThread != null, "Task thread was not started.");
        StreamTask<?, ?> streamTask = taskThread.task;
        while (!streamTask.isRunning()) {
            Thread.sleep(10);
            if (!taskThread.isAlive()) {
                if (taskThread.getError() != null) {
                    throw new Exception(
                            "Task Thread failed due to an error.", taskThread.getError());
                } else {
                    throw new Exception("Task Thread unexpectedly shut down.");
                }
            }
        }
    }

    public StreamTask<OUT, ?> getTask() {
        return taskThread.task;
    }

    public Thread getTaskThread() {
        return taskThread;
    }

    /**
     * Get all the output from the task. This contains StreamRecords and Events interleaved. Use
     * {@link
     * org.apache.flink.streaming.util.TestHarnessUtil#getRawElementsFromOutput(java.util.Queue)}}
     * to extract only the StreamRecords.
     */
    public LinkedBlockingQueue<Object> getOutput() {
        return outputList;
    }

    public StreamConfig getStreamConfig() {
        return streamConfig;
    }

    public ExecutionConfig getExecutionConfig() {
        return executionConfig;
    }

    private void shutdownIOManager() throws Exception {
        this.mockEnv.getIOManager().close();
    }

    private void shutdownMemoryManager() {
        if (this.memorySize > 0) {
            MemoryManager memMan = this.mockEnv.getMemoryManager();
            if (memMan != null) {
                Assert.assertTrue(
                        "Memory Manager managed memory was not completely freed.",
                        memMan.verifyEmpty());
                memMan.shutdown();
            }
        }
    }

    /** Sends the element to input gate 0 on channel 0. */
    public void processElement(Object element) {
        inputGates[0].sendElement(element, 0);
    }

    /** Sends the element to the specified channel on the specified input gate. */
    public void processElement(Object element, int inputGate, int channel) {
        inputGates[inputGate].sendElement(element, channel);
    }

    /** Sends the event to input gate 0 on channel 0. */
    public void processEvent(AbstractEvent event) {
        inputGates[0].sendEvent(event, 0);
    }

    /** Sends the event to the specified channel on the specified input gate. */
    public void processEvent(AbstractEvent event, int inputGate, int channel) {
        inputGates[inputGate].sendEvent(event, channel);
    }

    /** This only returns after all input queues are empty. */
    public void waitForInputProcessing() throws Exception {
        while (taskThread.isAlive()) {
            boolean allEmpty = true;
            for (int i = 0; i < numInputGates; i++) {
                if (!inputGates[i].allQueuesEmpty()) {
                    allEmpty = false;
                }
            }

            if (allEmpty) {
                break;
            }
        }

        // Wait for all currently available input has been processed.
        final AtomicBoolean allInputProcessed = new AtomicBoolean();
        final MailboxProcessor mailboxProcessor = taskThread.task.mailboxProcessor;
        final MailboxExecutor mailboxExecutor = mailboxProcessor.getMainMailboxExecutor();
        while (taskThread.isAlive()) {
            try {
                final CountDownLatch latch = new CountDownLatch(1);
                mailboxExecutor.execute(
                        () -> {
                            allInputProcessed.set(!mailboxProcessor.isDefaultActionAvailable());
                            latch.countDown();
                        },
                        "query-whether-processInput-has-suspend-itself");
                // Mail could be dropped due to task exception, so we do timed-await here.
                latch.await(1, TimeUnit.SECONDS);
            } catch (RejectedExecutionException ex) {
                // Loop until task thread exit for possible task exception.
            }

            if (allInputProcessed.get()) {
                break;
            }

            try {
                Thread.sleep(1);
            } catch (InterruptedException ignored) {
            }
        }

        Throwable error = taskThread.getError();
        if (error != null) {
            throw new Exception("Exception in the task thread", error);
        }
    }

    /**
     * Notifies all input channels on all input gates that no more input will arrive. This will
     * usually make the Task exit from his internal loop.
     */
    public void endInput() {
        for (int i = 0; i < numInputGates; i++) {
            inputGates[i].endInput();
        }
    }

    /**
     * Notifies the specified input channel on the specified input gate that no more data will
     * arrive.
     */
    public void endInput(int gateIndex, int channelIndex) {
        endInput(gateIndex, channelIndex, true);
    }

    public void endInput(int gateIndex, int channelIndex, boolean emitEndOfData) {
        if (emitEndOfData) {
            inputGates[gateIndex].sendEvent(new EndOfData(StopMode.DRAIN), channelIndex);
        }
        inputGates[gateIndex].sendEvent(EndOfPartitionEvent.INSTANCE, channelIndex);
    }

    public StreamConfigChainer<StreamTaskTestHarness<OUT>> setupOperatorChain(
            OperatorID headOperatorId, StreamOperator<?> headOperator) {
        return setupOperatorChain(headOperatorId, SimpleOperatorFactory.of(headOperator));
    }

    public StreamConfigChainer<StreamTaskTestHarness<OUT>> setupOperatorChain(
            OperatorID headOperatorId, StreamOperatorFactory<?> headOperatorFactory) {
        Preconditions.checkState(!setupCalled, "This harness was already setup.");
        setupCalled = true;
        StreamConfig streamConfig = getStreamConfig();
        streamConfig.setStreamOperatorFactory(headOperatorFactory);
        streamConfig.serializeAllConfigs();
        return new StreamConfigChainer(headOperatorId, streamConfig, this, 1);
    }

    // ------------------------------------------------------------------------

    class TaskThread extends Thread {

        private final SupplierWithException<? extends StreamTask<OUT, ?>, Exception> taskFactory;
        private volatile StreamTask<OUT, ?> task;

        private volatile Throwable error;

        TaskThread(SupplierWithException<? extends StreamTask<OUT, ?>, Exception> taskFactory) {
            super("Task Thread");
            this.taskFactory = taskFactory;
        }

        @Override
        public void run() {
            try {
                task = taskFactory.get();
                task.invoke();
                shutdownIOManager();
                shutdownMemoryManager();
            } catch (Throwable throwable) {
                this.error = throwable;
            } finally {
                try {
                    task.cleanUp(this.error);
                } catch (Exception cleanUpException) {
                    if (this.error == null) {
                        this.error = cleanUpException;
                    } else {
                        this.error.addSuppressed(cleanUpException);
                    }
                }
            }
        }

        public Throwable getError() {
            return error;
        }
    }

    static TaskMetricGroup createTaskMetricGroup(Map<String, Metric> metrics) {
        return TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        new TestMetricRegistry(metrics), "localhost", ResourceID.generate())
                .addJob(new JobID(), "jobName")
                .addTask(createExecutionAttemptId(), "test");
    }

    /** The metric registry for storing the registered metrics to verify in tests. */
    static class TestMetricRegistry extends NoOpMetricRegistry {
        private final Map<String, Metric> metrics;

        TestMetricRegistry(Map<String, Metric> metrics) {
            super();
            this.metrics = metrics;
        }

        @Override
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {
            metrics.put(metricName, metric);
        }
    }
}
