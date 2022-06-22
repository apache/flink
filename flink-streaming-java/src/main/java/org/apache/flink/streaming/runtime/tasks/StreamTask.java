/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.security.FlinkSecurityManager;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetricsBuilder;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReader;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.writer.MultipleRecordWriters;
import org.apache.flink.runtime.io.network.api.writer.NonRecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.SingleRecordWriter;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointableTask;
import org.apache.flink.runtime.jobgraph.tasks.CoordinatedTask;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLoader;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManagerImpl;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.bufferdebloat.BufferDebloater;
import org.apache.flink.streaming.runtime.tasks.mailbox.GaugePeriodTimer;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.PeriodTimer;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.WrappingRuntimeException;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.RunnableWithException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;

import static org.apache.flink.configuration.TaskManagerOptions.BUFFER_DEBLOAT_PERIOD;
import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.flink.util.concurrent.FutureUtils.assertNoException;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed and
 * executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form the
 * Task's operator chain. Operators that are chained together execute synchronously in the same
 * thread and hence on the same stream partition. A common case for these chains are successive
 * map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators. The StreamTask is
 * specialized for the type of the head operator: one-input and two-input tasks, as well as for
 * sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * <p>The life cycle of the task is set up as follows:
 *
 * <pre>{@code
 * -- setInitialState -> provides state of all operators in the chain
 *
 * -- invoke()
 *       |
 *       +----> Create basic utils (config, etc) and load the chain of operators
 *       +----> operators.setup()
 *       +----> task specific init()
 *       +----> initialize-operator-states()
 *       +----> open-operators()
 *       +----> run()
 *       +----> finish-operators()
 *       +----> close-operators()
 *       +----> common cleanup
 *       +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a {@code
 * StreamOperator} must be synchronized on this lock object to ensure that no methods are called
 * concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
        implements TaskInvokable,
                CheckpointableTask,
                CoordinatedTask,
                AsyncExceptionHandler,
                ContainingTaskDetails {

    /** The thread group that holds all trigger timer threads. */
    public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

    /** The logger used by the StreamTask and its subclasses. */
    protected static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

    // ------------------------------------------------------------------------

    /**
     * All actions outside of the task {@link #mailboxProcessor mailbox} (i.e. performed by another
     * thread) must be executed through this executor to ensure that we don't have concurrent method
     * calls that void consistent checkpoints. The execution will always be performed in the task
     * thread.
     *
     * <p>CheckpointLock is superseded by {@link MailboxExecutor}, with {@link
     * StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor
     * SynchronizedStreamTaskActionExecutor} to provide lock to {@link SourceStreamTask}.
     */
    private final StreamTaskActionExecutor actionExecutor;

    /** The input processor. Initialized in {@link #init()} method. */
    @Nullable protected StreamInputProcessor inputProcessor;

    /** the main operator that consumes the input streams of this task. */
    protected OP mainOperator;

    /** The chain of operators executed by this task. */
    protected OperatorChain<OUT, OP> operatorChain;

    /** The configuration of this streaming task. */
    protected final StreamConfig configuration;

    /** Our state backend. We use this to create a keyed state backend. */
    protected final StateBackend stateBackend;

    /** Our checkpoint storage. We use this to create checkpoint streams. */
    protected final CheckpointStorage checkpointStorage;

    private final SubtaskCheckpointCoordinator subtaskCheckpointCoordinator;

    /**
     * The internal {@link TimerService} used to define the current processing time (default =
     * {@code System.currentTimeMillis()}) and register timers for tasks to be executed in the
     * future.
     */
    protected final TimerService timerService;

    /**
     * In contrast to {@link #timerService} we should not register any user timers here. It should
     * be used only for system level timers.
     */
    protected final TimerService systemTimerService;

    /** The currently active background materialization threads. */
    private final CloseableRegistry cancelables = new CloseableRegistry();

    private final StreamTaskAsyncExceptionHandler asyncExceptionHandler;

    /**
     * Flag to mark the task "in operation", in which case check needs to be initialized to true, so
     * that early cancel() before invoke() behaves correctly.
     */
    private volatile boolean isRunning;

    /** Flag to mark this task as canceled. */
    private volatile boolean canceled;

    /**
     * Flag to mark this task as failing, i.e. if an exception has occurred inside {@link
     * #invoke()}.
     */
    private volatile boolean failing;

    /** Flags indicating the finished method of all the operators are called. */
    private boolean finishedOperators;

    private boolean closedOperators;

    /** Thread pool for async snapshot workers. */
    private final ExecutorService asyncOperationsThreadPool;

    private final RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriter;

    protected final MailboxProcessor mailboxProcessor;

    final MailboxExecutor mainMailboxExecutor;

    /** TODO it might be replaced by the global IO executor on TaskManager level future. */
    private final ExecutorService channelIOExecutor;

    // ========================================================
    //  Final  checkpoint / savepoint
    // ========================================================
    private Long syncSavepointWithoutDrain = null;
    private Long syncSavepointWithDrain = null;
    private Long finalCheckpointMinId = null;
    private final CompletableFuture<Void> finalCheckpointCompleted = new CompletableFuture<>();

    private long latestReportCheckpointId = -1;

    private long latestAsyncCheckpointStartDelayNanos;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();
    private volatile boolean endOfDataReceived = false;

    private final ThroughputCalculator throughputCalculator;

    private final @Nullable BufferDebloater bufferDebloater;

    private final long bufferDebloatPeriod;

    private final Environment environment;

    private final Object shouldInterruptOnCancelLock = new Object();

    @GuardedBy("shouldInterruptOnCancelLock")
    private boolean shouldInterruptOnCancel = true;

    // ------------------------------------------------------------------------

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     */
    protected StreamTask(Environment env) throws Exception {
        this(env, null);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * @param env The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.
     */
    protected StreamTask(Environment env, @Nullable TimerService timerService) throws Exception {
        this(env, timerService, FatalExitExceptionHandler.INSTANCE);
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                StreamTaskActionExecutor.IMMEDIATE);
    }

    /**
     * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
     *
     * <p>This constructor accepts a special {@link TimerService}. By default (and if null is passes
     * for the timer service) a {@link SystemProcessingTimeService DefaultTimerService} will be
     * used.
     *
     * @param environment The task environment for this task.
     * @param timerService Optionally, a specific timer service to use.
     * @param uncaughtExceptionHandler to handle uncaught exceptions in the async operations thread
     *     pool
     * @param actionExecutor a mean to wrap all actions performed by this task thread. Currently,
     *     only SynchronizedActionExecutor can be used to preserve locking semantics.
     */
    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor)
            throws Exception {
        this(
                environment,
                timerService,
                uncaughtExceptionHandler,
                actionExecutor,
                new TaskMailboxImpl(Thread.currentThread()));
    }

    protected StreamTask(
            Environment environment,
            @Nullable TimerService timerService,
            Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
            StreamTaskActionExecutor actionExecutor,
            TaskMailbox mailbox)
            throws Exception {
        this.environment = environment;
        this.configuration = new StreamConfig(environment.getTaskConfiguration());
        this.recordWriter = createRecordWriterDelegate(configuration, environment);
        this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
        this.mailboxProcessor = new MailboxProcessor(this::processInput, mailbox, actionExecutor);
        this.mainMailboxExecutor = mailboxProcessor.getMainMailboxExecutor();
        this.asyncExceptionHandler = new StreamTaskAsyncExceptionHandler(environment);
        this.asyncOperationsThreadPool =
                Executors.newCachedThreadPool(
                        new ExecutorThreadFactory("AsyncOperations", uncaughtExceptionHandler));

        environment.setMainMailboxExecutor(mainMailboxExecutor);
        environment.setAsyncOperationsThreadPool(asyncOperationsThreadPool);

        this.stateBackend = createStateBackend();
        this.checkpointStorage = createCheckpointStorage(stateBackend);

        this.subtaskCheckpointCoordinator =
                new SubtaskCheckpointCoordinatorImpl(
                        checkpointStorage.createCheckpointStorage(environment.getJobID()),
                        getName(),
                        actionExecutor,
                        getCancelables(),
                        getAsyncOperationsThreadPool(),
                        environment,
                        this,
                        configuration.isUnalignedCheckpointsEnabled(),
                        configuration
                                .getConfiguration()
                                .get(
                                        ExecutionCheckpointingOptions
                                                .ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH),
                        this::prepareInputSnapshot);

        // if the clock is not already set, then assign a default TimeServiceProvider
        if (timerService == null) {
            this.timerService = createTimerService("Time Trigger for " + getName());
        } else {
            this.timerService = timerService;
        }

        this.systemTimerService = createTimerService("System Time Trigger for " + getName());
        this.channelIOExecutor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory("channel-state-unspilling"));

        injectChannelStateWriterIntoChannels();

        environment.getMetricGroup().getIOMetricGroup().setEnableBusyTime(true);
        this.throughputCalculator = environment.getThroughputCalculator();
        Configuration taskManagerConf = environment.getTaskManagerInfo().getConfiguration();

        this.bufferDebloatPeriod = taskManagerConf.get(BUFFER_DEBLOAT_PERIOD).toMillis();

        if (taskManagerConf.get(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED)) {
            this.bufferDebloater =
                    new BufferDebloater(taskManagerConf, environment.getAllInputGates());
            environment
                    .getMetricGroup()
                    .gauge(
                            MetricNames.ESTIMATED_TIME_TO_CONSUME_BUFFERS,
                            () ->
                                    bufferDebloater
                                            .getLastEstimatedTimeToConsumeBuffers()
                                            .toMillis());
            environment
                    .getMetricGroup()
                    .gauge(MetricNames.DEBLOATED_BUFFER_SIZE, bufferDebloater::getLastBufferSize);
        } else {
            this.bufferDebloater = null;
        }
    }

    private TimerService createTimerService(String timerThreadName) {
        ThreadFactory timerThreadFactory =
                new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, timerThreadName);
        return new SystemProcessingTimeService(this::handleTimerException, timerThreadFactory);
    }

    private void injectChannelStateWriterIntoChannels() {
        final Environment env = getEnvironment();
        final ChannelStateWriter channelStateWriter =
                subtaskCheckpointCoordinator.getChannelStateWriter();
        for (final InputGate gate : env.getAllInputGates()) {
            gate.setChannelStateWriter(channelStateWriter);
        }
        for (ResultPartitionWriter writer : env.getAllWriters()) {
            if (writer instanceof ChannelStateHolder) {
                ((ChannelStateHolder) writer).setChannelStateWriter(channelStateWriter);
            }
        }
    }

    private CompletableFuture<Void> prepareInputSnapshot(
            ChannelStateWriter channelStateWriter, long checkpointId) throws CheckpointException {
        if (inputProcessor == null) {
            return FutureUtils.completedVoidFuture();
        }
        return inputProcessor.prepareSnapshot(channelStateWriter, checkpointId);
    }

    SubtaskCheckpointCoordinator getCheckpointCoordinator() {
        return subtaskCheckpointCoordinator;
    }

    // ------------------------------------------------------------------------
    //  Life cycle methods for specific implementations
    // ------------------------------------------------------------------------

    protected abstract void init() throws Exception;

    protected void cancelTask() throws Exception {}

    /**
     * This method implements the default action of the task (e.g. processing one event from the
     * input). Implementations should (in general) be non-blocking.
     *
     * @param controller controller object for collaborative interaction between the action and the
     *     stream task.
     * @throws Exception on any problems in the action.
     */
    protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
        DataInputStatus status = inputProcessor.processInput();
        switch (status) {
            case MORE_AVAILABLE:
                if (recordWriter.isAvailable()) {
                    return;
                }
                break;
            case NOTHING_AVAILABLE:
                break;
            case END_OF_RECOVERY:
                throw new IllegalStateException("We should not receive this event here.");
            case END_OF_DATA:
                endData();
                return;
            case END_OF_INPUT:
                // Suspend the mailbox processor, it would be resumed in afterInvoke and finished
                // after all records processed by the downstream tasks. We also suspend the default
                // actions to avoid repeat executing the empty default operation (namely process
                // records).
                controller.suspendDefaultAction();
                mailboxProcessor.suspend();
                return;
        }

        TaskIOMetricGroup ioMetrics = getEnvironment().getMetricGroup().getIOMetricGroup();
        PeriodTimer timer;
        CompletableFuture<?> resumeFuture;
        if (!recordWriter.isAvailable()) {
            timer = new GaugePeriodTimer(ioMetrics.getBackPressuredTimePerSecond());
            resumeFuture = recordWriter.getAvailableFuture();
        } else {
            timer =
                    new ThroughputPeriodTimer(
                            ioMetrics.getIdleTimeMsPerSecond(), throughputCalculator);
            resumeFuture = inputProcessor.getAvailableFuture();
        }
        assertNoException(resumeFuture.thenRun(controller.suspendDefaultAction(timer)::resume));
    }

    protected void endData() throws Exception {

        if (syncSavepointWithoutDrain != null && areCheckpointsWithFinishedTasksEnabled()) {
            throw new FlinkRuntimeException(
                    "We run out of data to process while waiting for a synchronous savepoint"
                            + " to be finished. This can lead to a deadlock waiting for a final"
                            + " checkpoint after a synchronous savepoint, which will never be"
                            + " triggered.");
        }

        advanceToEndOfEventTime();
        // finish all operators in a chain effect way
        operatorChain.finishOperators(actionExecutor);
        this.finishedOperators = true;

        for (ResultPartitionWriter partitionWriter : getEnvironment().getAllWriters()) {
            partitionWriter.notifyEndOfData();
        }

        this.endOfDataReceived = true;
    }

    protected void setSynchronousSavepoint(long checkpointId, boolean isDrain) {
        checkState(
                syncSavepointWithoutDrain == null
                        && (syncSavepointWithDrain == null
                                || (isDrain && syncSavepointWithDrain == checkpointId)),
                "at most one stop-with-savepoint checkpoint at a time is allowed");
        if (isDrain) {
            if (syncSavepointWithDrain == null) {
                syncSavepointWithDrain = checkpointId;
            }
        } else {
            syncSavepointWithoutDrain = checkpointId;
        }
    }

    @VisibleForTesting
    OptionalLong getSynchronousSavepointId() {
        if (syncSavepointWithoutDrain != null) {
            return OptionalLong.of(syncSavepointWithoutDrain);
        } else if (syncSavepointWithDrain != null) {
            return OptionalLong.of(syncSavepointWithDrain);
        } else {
            return OptionalLong.empty();
        }
    }

    private boolean isCurrentSavepointWithDrain(long checkpointId) {
        return syncSavepointWithDrain != null && syncSavepointWithDrain == checkpointId;
    }

    private boolean isCurrentSavepointWithoutDrain(long checkpointId) {
        return syncSavepointWithoutDrain != null && syncSavepointWithoutDrain == checkpointId;
    }

    private void runSynchronousSavepointMailboxLoop() throws Exception {
        assert syncSavepointWithoutDrain != null;

        MailboxExecutor mailboxExecutor =
                mailboxProcessor.getMailboxExecutor(TaskMailbox.MAX_PRIORITY);

        while (!canceled && syncSavepointWithoutDrain != null) {
            mailboxExecutor.yield();
        }
    }

    /**
     * Emits the {@link org.apache.flink.streaming.api.watermark.Watermark#MAX_WATERMARK
     * MAX_WATERMARK} so that all registered timers are fired.
     *
     * <p>This is used by the source task when the job is {@code TERMINATED}. In the case, we want
     * all the timers registered throughout the pipeline to fire and the related state (e.g.
     * windows) to be flushed.
     *
     * <p>For tasks other than the source task, this method does nothing.
     */
    protected void advanceToEndOfEventTime() throws Exception {}

    /**
     * Instructs the task to go through its normal termination routine, i.e. exit the run-loop and
     * call {@link StreamOperator#finish()} and {@link StreamOperator#close()} on its operators.
     *
     * <p>This is used by the source task to get out of the run-loop when the job is stopped with a
     * savepoint.
     *
     * <p>For tasks other than the source task, this method does nothing.
     */
    protected void finishTask() throws Exception {}

    // ------------------------------------------------------------------------
    //  Core work methods of the Stream Task
    // ------------------------------------------------------------------------

    public StreamTaskStateInitializer createStreamTaskStateInitializer() {
        InternalTimeServiceManager.Provider timerServiceProvider =
                configuration.getTimerServiceProvider(getUserCodeClassLoader());
        return new StreamTaskStateInitializerImpl(
                getEnvironment(),
                stateBackend,
                TtlTimeProvider.DEFAULT,
                timerServiceProvider != null
                        ? timerServiceProvider
                        : InternalTimeServiceManagerImpl::create);
    }

    protected Counter setupNumRecordsInCounter(StreamOperator streamOperator) {
        try {
            return streamOperator.getMetricGroup().getIOMetricGroup().getNumRecordsInCounter();
        } catch (Exception e) {
            LOG.warn("An exception occurred during the metrics setup.", e);
            return new SimpleCounter();
        }
    }

    @Override
    public final void restore() throws Exception {
        restoreInternal();
    }

    void restoreInternal() throws Exception {
        if (isRunning) {
            LOG.debug("Re-restore attempt rejected.");
            return;
        }
        closedOperators = false;
        LOG.debug("Initializing {}.", getName());

        operatorChain =
                getEnvironment().getTaskStateManager().isTaskDeployedAsFinished()
                        ? new FinishedOperatorChain<>(this, recordWriter)
                        : new RegularOperatorChain<>(this, recordWriter);
        mainOperator = operatorChain.getMainOperator();

        getEnvironment()
                .getTaskStateManager()
                .getRestoreCheckpointId()
                .ifPresent(restoreId -> latestReportCheckpointId = restoreId);

        // task specific initialization
        init();

        // save the work of reloading state, etc, if the task is already canceled
        ensureNotCanceled();

        // -------- Invoke --------
        LOG.debug("Invoking {}", getName());

        // we need to make sure that any triggers scheduled in open() cannot be
        // executed before all operators are opened
        CompletableFuture<Void> allGatesRecoveredFuture = actionExecutor.call(this::restoreGates);

        // Run mailbox until all gates will be recovered.
        mailboxProcessor.runMailboxLoop();

        ensureNotCanceled();

        checkState(
                allGatesRecoveredFuture.isDone(),
                "Mailbox loop interrupted before recovery was finished.");

        // we recovered all the gates, we can close the channel IO executor as it is no longer
        // needed
        channelIOExecutor.shutdown();

        isRunning = true;
    }

    private CompletableFuture<Void> restoreGates() throws Exception {
        SequentialChannelStateReader reader =
                getEnvironment().getTaskStateManager().getSequentialChannelStateReader();
        reader.readOutputData(
                getEnvironment().getAllWriters(), !configuration.isGraphContainingLoops());

        operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());

        IndexedInputGate[] inputGates = getEnvironment().getAllInputGates();
        channelIOExecutor.execute(
                () -> {
                    try {
                        reader.readInputData(inputGates);
                    } catch (Exception e) {
                        asyncExceptionHandler.handleAsyncException(
                                "Unable to read channel state", e);
                    }
                });

        List<CompletableFuture<?>> recoveredFutures = new ArrayList<>(inputGates.length);
        for (InputGate inputGate : inputGates) {
            recoveredFutures.add(inputGate.getStateConsumedFuture());

            inputGate
                    .getStateConsumedFuture()
                    .thenRun(
                            () ->
                                    mainMailboxExecutor.execute(
                                            inputGate::requestPartitions,
                                            "Input gate request partitions"));
        }

        return CompletableFuture.allOf(recoveredFutures.toArray(new CompletableFuture[0]))
                .thenRun(mailboxProcessor::suspend);
    }

    private void ensureNotCanceled() {
        if (canceled) {
            throw new CancelTaskException();
        }
    }

    @Override
    public final void invoke() throws Exception {
        // Allow invoking method 'invoke' without having to call 'restore' before it.
        if (!isRunning) {
            LOG.debug("Restoring during invoke will be called.");
            restoreInternal();
        }

        // final check to exit early before starting to run
        ensureNotCanceled();

        scheduleBufferDebloater();

        // let the task do its work
        runMailboxLoop();

        // if this left the run() method cleanly despite the fact that this was canceled,
        // make sure the "clean shutdown" is not attempted
        ensureNotCanceled();

        afterInvoke();
    }

    private void scheduleBufferDebloater() {
        // See https://issues.apache.org/jira/browse/FLINK-23560
        // If there are no input gates, there is no point of calculating the throughput and running
        // the debloater. At the same time, for SourceStreamTask using legacy sources and checkpoint
        // lock, enqueuing even a single mailbox action can cause performance regression. This is
        // especially visible in batch, with disabled checkpointing and no processing time timers.
        if (getEnvironment().getAllInputGates().length == 0) {
            return;
        }
        systemTimerService.registerTimer(
                systemTimerService.getCurrentProcessingTime() + bufferDebloatPeriod,
                timestamp ->
                        mainMailboxExecutor.execute(
                                () -> {
                                    debloat();
                                    scheduleBufferDebloater();
                                },
                                "Buffer size recalculation"));
    }

    @VisibleForTesting
    void debloat() {
        long throughput = throughputCalculator.calculateThroughput();
        if (bufferDebloater != null) {
            bufferDebloater.recalculateBufferSize(throughput);
        }
    }

    @VisibleForTesting
    public boolean runMailboxStep() throws Exception {
        return mailboxProcessor.runMailboxStep();
    }

    @VisibleForTesting
    public boolean isMailboxLoopRunning() {
        return mailboxProcessor.isMailboxLoopRunning();
    }

    public void runMailboxLoop() throws Exception {
        mailboxProcessor.runMailboxLoop();
    }

    protected void afterInvoke() throws Exception {
        LOG.debug("Finished task {}", getName());
        getCompletionFuture().exceptionally(unused -> null).join();

        Set<CompletableFuture<Void>> terminationConditions = new HashSet<>();
        // If checkpoints are enabled, waits for all the records get processed by the downstream
        // tasks. During this process, this task could coordinate with its downstream tasks to
        // continue perform checkpoints.
        if (endOfDataReceived && areCheckpointsWithFinishedTasksEnabled()) {
            LOG.debug("Waiting for all the records processed by the downstream tasks.");

            for (ResultPartitionWriter partitionWriter : getEnvironment().getAllWriters()) {
                terminationConditions.add(partitionWriter.getAllDataProcessedFuture());
            }

            terminationConditions.add(finalCheckpointCompleted);
        }

        if (syncSavepointWithDrain != null) {
            terminationConditions.add(finalCheckpointCompleted);
        }

        FutureUtils.waitForAll(terminationConditions)
                .thenRun(mailboxProcessor::allActionsCompleted);

        // Resumes the mailbox processor. The mailbox processor would be completed
        // after all records are processed by the downstream tasks.
        mailboxProcessor.runMailboxLoop();

        // make sure no further checkpoint and notification actions happen.
        // at the same time, this makes sure that during any "regular" exit where still
        actionExecutor.runThrowing(
                () -> {
                    // make sure no new timers can come
                    timerService.quiesce().get();
                    systemTimerService.quiesce().get();

                    // let mailbox execution reject all new letters from this point
                    mailboxProcessor.prepareClose();
                });

        // processes the remaining mails; no new mails can be enqueued
        mailboxProcessor.drain();

        // Set isRunning to false after all the mails are drained so that
        // the queued checkpoint requirements could be triggered normally.
        actionExecutor.runThrowing(
                () -> {
                    // only set the StreamTask to not running after all operators have been
                    // finished!
                    // See FLINK-7430
                    isRunning = false;
                });

        LOG.debug("Finished operators for task {}", getName());

        // make sure all buffered data is flushed
        operatorChain.flushOutputs();

        if (areCheckpointsWithFinishedTasksEnabled()) {
            // No new checkpoints could be triggered since mailbox has been drained.
            subtaskCheckpointCoordinator.waitForPendingCheckpoints();
            LOG.debug("All pending checkpoints are finished");
        }

        // make an attempt to dispose the operators such that failures in the dispose call
        // still let the computation fail
        closeAllOperators();
    }

    private boolean areCheckpointsWithFinishedTasksEnabled() {
        return configuration
                        .getConfiguration()
                        .get(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH)
                && configuration.isCheckpointingEnabled();
    }

    @Override
    public final void cleanUp(Throwable throwable) throws Exception {
        LOG.debug(
                "Cleanup StreamTask (operators closed: {}, cancelled: {})",
                closedOperators,
                canceled);

        failing = !canceled && throwable != null;

        Exception suppressedException =
                throwable == null ? null : runAndSuppressThrowable(this::cancelTask, null);

        getCompletionFuture().exceptionally(unused -> null).join();
        // clean up everything we initialized
        isRunning = false;

        // Now that we are outside the user code, we do not want to be interrupted further
        // upon cancellation. The shutdown logic below needs to make sure it does not issue calls
        // that block and stall shutdown.
        // Additionally, the cancellation watch dog will issue a hard-cancel (kill the TaskManager
        // process) as a backup in case some shutdown procedure blocks outside our control.
        disableInterruptOnCancel();

        // clear any previously issued interrupt for a more graceful shutdown
        Thread.interrupted();

        // stop all timers and threads
        suppressedException =
                runAndSuppressThrowable(this::tryShutdownTimerService, suppressedException);

        // stop all asynchronous checkpoint threads
        suppressedException = runAndSuppressThrowable(cancelables::close, suppressedException);
        suppressedException =
                runAndSuppressThrowable(this::shutdownAsyncThreads, suppressedException);

        // we must! perform this cleanup
        suppressedException = runAndSuppressThrowable(this::cleanUpInternal, suppressedException);

        // if the operators were not closed before, do a hard close
        suppressedException = runAndSuppressThrowable(this::closeAllOperators, suppressedException);

        // release the output resources. this method should never fail.
        suppressedException =
                runAndSuppressThrowable(this::releaseOutputResources, suppressedException);

        suppressedException =
                runAndSuppressThrowable(channelIOExecutor::shutdown, suppressedException);

        suppressedException = runAndSuppressThrowable(mailboxProcessor::close, suppressedException);

        if (suppressedException == null) {
            terminationFuture.complete(null);
        } else {
            terminationFuture.completeExceptionally(suppressedException);
            throw suppressedException;
        }
    }

    protected void cleanUpInternal() throws Exception {
        if (inputProcessor != null) {
            inputProcessor.close();
        }
    }

    protected CompletableFuture<Void> getCompletionFuture() {
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public final Future<Void> cancel() throws Exception {
        isRunning = false;
        canceled = true;

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        // the "cancel task" call must come first, but the cancelables must be
        // closed no matter what
        try {
            cancelTask();
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
            getCompletionFuture()
                    .whenComplete(
                            (unusedResult, unusedError) -> {
                                // WARN: the method is called from the task thread but the callback
                                // can be invoked from a different thread
                                mailboxProcessor.allActionsCompleted();
                                try {
                                    cancelables.close();
                                } catch (IOException e) {
                                    throw new CompletionException(e);
                                }
                            });
        }
        return terminationFuture;
    }

    public MailboxExecutorFactory getMailboxExecutorFactory() {
        return this.mailboxProcessor::getMailboxExecutor;
    }

    public final boolean isRunning() {
        return isRunning;
    }

    public final boolean isCanceled() {
        return canceled;
    }

    public final boolean isFailing() {
        return failing;
    }

    private void shutdownAsyncThreads() throws Exception {
        if (!asyncOperationsThreadPool.isShutdown()) {
            asyncOperationsThreadPool.shutdownNow();
        }
    }

    private void releaseOutputResources() throws Exception {
        if (operatorChain != null) {
            // beware: without synchronization, #performCheckpoint() may run in
            //         parallel and this call is not thread-safe
            actionExecutor.run(() -> operatorChain.close());
        } else {
            // failed to allocate operatorChain, clean up record writers
            recordWriter.close();
        }
    }

    private Exception runAndSuppressThrowable(
            ThrowingRunnable<?> runnable, @Nullable Exception originalException) {
        try {
            runnable.run();
        } catch (Throwable t) {
            // TODO: investigate why Throwable instead of Exception is used here.
            Exception e = t instanceof Exception ? (Exception) t : new Exception(t);
            return firstOrSuppressed(e, originalException);
        }

        return originalException;
    }

    /** Closes all the operators if not closed before. */
    private void closeAllOperators() throws Exception {
        if (operatorChain != null && !closedOperators) {
            closedOperators = true;
            operatorChain.closeAllOperators();
        }
    }

    /**
     * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
     * shutdown method was never called.
     *
     * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
     * shutdown is attempted, and cause threads to linger for longer than needed.
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        if (!timerService.isTerminated()) {
            LOG.info("Timer service is shutting down.");
            timerService.shutdownService();
        }

        if (!systemTimerService.isTerminated()) {
            LOG.info("System timer service is shutting down.");
            systemTimerService.shutdownService();
        }

        cancelables.close();
    }

    boolean isSerializingTimestamps() {
        TimeCharacteristic tc = configuration.getTimeCharacteristic();
        return tc == TimeCharacteristic.EventTime | tc == TimeCharacteristic.IngestionTime;
    }

    // ------------------------------------------------------------------------
    //  Access to properties and utilities
    // ------------------------------------------------------------------------

    /**
     * Gets the name of the task, in the form "taskname (2/5)".
     *
     * @return The name of the task.
     */
    public final String getName() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
    }

    /**
     * Gets the name of the task, appended with the subtask indicator and execution id.
     *
     * @return The name of the task, with subtask indicator and execution id.
     */
    String getTaskNameWithSubtaskAndId() {
        return getEnvironment().getTaskInfo().getTaskNameWithSubtasks()
                + " ("
                + getEnvironment().getExecutionId()
                + ')';
    }

    public CheckpointStorageWorkerView getCheckpointStorage() {
        return subtaskCheckpointCoordinator.getCheckpointStorage();
    }

    public StreamConfig getConfiguration() {
        return configuration;
    }

    RecordWriterOutput<?>[] getStreamOutputs() {
        return operatorChain.getStreamOutputs();
    }

    // ------------------------------------------------------------------------
    //  Checkpoint and Restore
    // ------------------------------------------------------------------------

    @Override
    public CompletableFuture<Boolean> triggerCheckpointAsync(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions) {

        CompletableFuture<Boolean> result = new CompletableFuture<>();
        mainMailboxExecutor.execute(
                () -> {
                    try {
                        boolean noUnfinishedInputGates =
                                Arrays.stream(getEnvironment().getAllInputGates())
                                        .allMatch(InputGate::isFinished);

                        if (noUnfinishedInputGates) {
                            result.complete(
                                    triggerCheckpointAsyncInMailbox(
                                            checkpointMetaData, checkpointOptions));
                        } else {
                            result.complete(
                                    triggerUnfinishedChannelsCheckpoint(
                                            checkpointMetaData, checkpointOptions));
                        }
                    } catch (Exception ex) {
                        // Report the failure both via the Future result but also to the mailbox
                        result.completeExceptionally(ex);
                        throw ex;
                    }
                },
                "checkpoint %s with %s",
                checkpointMetaData,
                checkpointOptions);
        return result;
    }

    private boolean triggerCheckpointAsyncInMailbox(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws Exception {
        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            latestAsyncCheckpointStartDelayNanos =
                    1_000_000
                            * Math.max(
                                    0,
                                    System.currentTimeMillis() - checkpointMetaData.getTimestamp());

            // No alignment if we inject a checkpoint
            CheckpointMetricsBuilder checkpointMetrics =
                    new CheckpointMetricsBuilder()
                            .setAlignmentDurationNanos(0L)
                            .setBytesProcessedDuringAlignment(0L)
                            .setCheckpointStartDelayNanos(latestAsyncCheckpointStartDelayNanos);

            subtaskCheckpointCoordinator.initInputsCheckpoint(
                    checkpointMetaData.getCheckpointId(), checkpointOptions);

            boolean success =
                    performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics);
            if (!success) {
                declineCheckpoint(checkpointMetaData.getCheckpointId());
            }
            return success;
        } catch (Exception e) {
            // propagate exceptions only if the task is still in "running" state
            if (isRunning) {
                throw new Exception(
                        "Could not perform checkpoint "
                                + checkpointMetaData.getCheckpointId()
                                + " for operator "
                                + getName()
                                + '.',
                        e);
            } else {
                LOG.debug(
                        "Could not perform checkpoint {} for operator {} while the "
                                + "invokable was not in state running.",
                        checkpointMetaData.getCheckpointId(),
                        getName(),
                        e);
                return false;
            }
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    private boolean triggerUnfinishedChannelsCheckpoint(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws Exception {
        Optional<CheckpointBarrierHandler> checkpointBarrierHandler = getCheckpointBarrierHandler();
        checkState(
                checkpointBarrierHandler.isPresent(),
                "CheckpointBarrier should exist for tasks with network inputs.");

        CheckpointBarrier barrier =
                new CheckpointBarrier(
                        checkpointMetaData.getCheckpointId(),
                        checkpointMetaData.getTimestamp(),
                        checkpointOptions);

        for (IndexedInputGate inputGate : getEnvironment().getAllInputGates()) {
            if (!inputGate.isFinished()) {
                for (InputChannelInfo channelInfo : inputGate.getUnfinishedChannels()) {
                    checkpointBarrierHandler.get().processBarrier(barrier, channelInfo, true);
                }
            }
        }

        return true;
    }

    protected final CompletableFuture<Boolean> assertTriggeringCheckpointExceptions(
            CompletableFuture<Boolean> triggerFuture, long checkpointId) {
        CompletableFuture<Boolean> checkpointTriggered =
                triggerFuture.exceptionally(
                        error -> {
                            if (ExceptionUtils.findThrowable(
                                            error, RejectedExecutionException.class)
                                    .isPresent()) {
                                // This may happen if the mailbox is closed. It means that
                                // the task is shutting down, so we just ignore it.
                                LOG.debug(
                                        "Triggering checkpoint {} for {} was rejected by the mailbox",
                                        checkpointId,
                                        getTaskNameWithSubtaskAndId());
                                return false;
                            } else {
                                throw new WrappingRuntimeException(error);
                            }
                        });
        FutureUtils.assertNoException(checkpointTriggered);
        return checkpointTriggered;
    }

    /**
     * Acquires the optional {@link CheckpointBarrierHandler} associated with this stream task. The
     * {@code CheckpointBarrierHandler} should exist if the task has data inputs and requires to
     * align the barriers.
     */
    protected Optional<CheckpointBarrierHandler> getCheckpointBarrierHandler() {
        return Optional.empty();
    }

    @Override
    public void triggerCheckpointOnBarrier(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws IOException {

        FlinkSecurityManager.monitorUserSystemExitForCurrentThread();
        try {
            if (performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics)) {
                if (isCurrentSavepointWithoutDrain(checkpointMetaData.getCheckpointId())) {
                    runSynchronousSavepointMailboxLoop();
                }
            }
        } catch (CancelTaskException e) {
            LOG.info(
                    "Operator {} was cancelled while performing checkpoint {}.",
                    getName(),
                    checkpointMetaData.getCheckpointId());
            throw e;
        } catch (Exception e) {
            throw new IOException(
                    "Could not perform checkpoint "
                            + checkpointMetaData.getCheckpointId()
                            + " for operator "
                            + getName()
                            + '.',
                    e);
        } finally {
            FlinkSecurityManager.unmonitorUserSystemExitForCurrentThread();
        }
    }

    @Override
    public void abortCheckpointOnBarrier(long checkpointId, CheckpointException cause)
            throws IOException {
        if (isCurrentSavepointWithoutDrain(checkpointId)) {
            syncSavepointWithoutDrain = null;
        } else if (isCurrentSavepointWithDrain(checkpointId)) {
            throw new FlinkRuntimeException("Stop-with-savepoint --drain failed.");
        }
        subtaskCheckpointCoordinator.abortCheckpointOnBarrier(checkpointId, cause, operatorChain);
    }

    private boolean performCheckpoint(
            CheckpointMetaData checkpointMetaData,
            CheckpointOptions checkpointOptions,
            CheckpointMetricsBuilder checkpointMetrics)
            throws Exception {

        final CheckpointType checkpointType = checkpointOptions.getCheckpointType();
        LOG.debug(
                "Starting checkpoint {} {} on task {}",
                checkpointMetaData.getCheckpointId(),
                checkpointType,
                getName());

        if (checkpointType.isSynchronous()
                && !checkpointType.shouldDrain()
                && endOfDataReceived
                && areCheckpointsWithFinishedTasksEnabled()) {
            LOG.debug("Can not trigger a stop-with-savepoint w/o drain if a task is finishing.");
            return false;
        }

        if (isRunning) {
            actionExecutor.runThrowing(
                    () -> {
                        if (checkpointType.isSynchronous()) {
                            setSynchronousSavepoint(
                                    checkpointMetaData.getCheckpointId(),
                                    checkpointType.shouldDrain());
                        }

                        if (areCheckpointsWithFinishedTasksEnabled()
                                && endOfDataReceived
                                && this.finalCheckpointMinId == null) {
                            this.finalCheckpointMinId = checkpointMetaData.getCheckpointId();
                        }

                        subtaskCheckpointCoordinator.checkpointState(
                                checkpointMetaData,
                                checkpointOptions,
                                checkpointMetrics,
                                operatorChain,
                                finishedOperators,
                                this::isRunning);
                    });

            return true;
        } else {
            actionExecutor.runThrowing(
                    () -> {
                        // we cannot perform our checkpoint - let the downstream operators know that
                        // they
                        // should not wait for any input from this operator

                        // we cannot broadcast the cancellation markers on the 'operator chain',
                        // because it may not
                        // yet be created
                        final CancelCheckpointMarker message =
                                new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
                        recordWriter.broadcastEvent(message);
                    });

            return false;
        }
    }

    protected void declineCheckpoint(long checkpointId) {
        getEnvironment()
                .declineCheckpoint(
                        checkpointId,
                        new CheckpointException(
                                "Task Name" + getName(),
                                CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY));
    }

    public final ExecutorService getAsyncOperationsThreadPool() {
        return asyncOperationsThreadPool;
    }

    @Override
    public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
        return notifyCheckpointOperation(
                () -> notifyCheckpointComplete(checkpointId),
                String.format("checkpoint %d complete", checkpointId));
    }

    @Override
    public Future<Void> notifyCheckpointAbortAsync(
            long checkpointId, long latestCompletedCheckpointId) {
        return notifyCheckpointOperation(
                () -> {
                    if (latestCompletedCheckpointId > 0) {
                        notifyCheckpointComplete(latestCompletedCheckpointId);
                    }

                    if (isCurrentSavepointWithoutDrain(checkpointId)) {
                        syncSavepointWithoutDrain = null;
                    } else if (isCurrentSavepointWithDrain(checkpointId)) {
                        throw new FlinkRuntimeException("Stop-with-savepoint --drain failed.");
                    }
                    subtaskCheckpointCoordinator.notifyCheckpointAborted(
                            checkpointId, operatorChain, this::isRunning);
                },
                String.format("checkpoint %d aborted", checkpointId));
    }

    private Future<Void> notifyCheckpointOperation(
            RunnableWithException runnable, String description) {
        CompletableFuture<Void> result = new CompletableFuture<>();
        mailboxProcessor
                .getMailboxExecutor(TaskMailbox.MAX_PRIORITY)
                .execute(
                        () -> {
                            try {
                                runnable.run();
                            } catch (Exception ex) {
                                result.completeExceptionally(ex);
                                throw ex;
                            }
                            result.complete(null);
                        },
                        description);
        return result;
    }

    private void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("Notify checkpoint {} complete on task {}", checkpointId, getName());

        if (checkpointId <= latestReportCheckpointId) {
            return;
        }

        latestReportCheckpointId = checkpointId;

        subtaskCheckpointCoordinator.notifyCheckpointComplete(
                checkpointId, operatorChain, this::isRunning);
        if (isRunning) {
            if (isCurrentSavepointWithoutDrain(checkpointId)) {
                finishTask();
                // Reset to "notify" the internal synchronous savepoint mailbox loop.
                syncSavepointWithoutDrain = null;
            } else if (isCurrentSavepointWithDrain(checkpointId)) {
                finalCheckpointCompleted.complete(null);
            } else if (syncSavepointWithDrain == null
                    && finalCheckpointMinId != null
                    && checkpointId >= finalCheckpointMinId) {
                finalCheckpointCompleted.complete(null);
            }
        }
    }

    private void tryShutdownTimerService() {
        final long timeoutMs =
                getEnvironment()
                        .getTaskManagerInfo()
                        .getConfiguration()
                        .getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT_TIMERS);
        tryShutdownTimerService(timeoutMs, timerService);
        tryShutdownTimerService(timeoutMs, systemTimerService);
    }

    private void tryShutdownTimerService(long timeoutMs, TimerService timerService) {
        if (!timerService.isTerminated()) {
            if (!timerService.shutdownServiceUninterruptible(timeoutMs)) {
                LOG.warn(
                        "Timer service shutdown exceeded time limit of {} ms while waiting for pending "
                                + "timers. Will continue with shutdown procedure.",
                        timeoutMs);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Operator Events
    // ------------------------------------------------------------------------

    @Override
    public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event)
            throws FlinkException {
        try {
            mainMailboxExecutor.execute(
                    () -> operatorChain.dispatchOperatorEvent(operator, event),
                    "dispatch operator event");
        } catch (RejectedExecutionException e) {
            // this happens during shutdown, we can swallow this
        }
    }

    // ------------------------------------------------------------------------
    //  State backend
    // ------------------------------------------------------------------------

    private StateBackend createStateBackend() throws Exception {
        final StateBackend fromApplication =
                configuration.getStateBackend(getUserCodeClassLoader());
        final TernaryBoolean isChangelogStateBackendEnableFromApplication =
                configuration.isChangelogStateBackendEnabled(getUserCodeClassLoader());

        return StateBackendLoader.fromApplicationOrConfigOrDefault(
                fromApplication,
                isChangelogStateBackendEnableFromApplication == null
                        ? TernaryBoolean.UNDEFINED
                        : isChangelogStateBackendEnableFromApplication,
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }

    private CheckpointStorage createCheckpointStorage(StateBackend backend) throws Exception {
        final CheckpointStorage fromApplication =
                configuration.getCheckpointStorage(getUserCodeClassLoader());
        final Path savepointDir = configuration.getSavepointDir(getUserCodeClassLoader());

        return CheckpointStorageLoader.load(
                fromApplication,
                savepointDir,
                backend,
                getEnvironment().getTaskManagerInfo().getConfiguration(),
                getUserCodeClassLoader(),
                LOG);
    }

    /**
     * Returns the {@link TimerService} responsible for telling the current processing time and
     * registering actual timers.
     */
    @VisibleForTesting
    TimerService getTimerService() {
        return timerService;
    }

    @VisibleForTesting
    OP getMainOperator() {
        return this.mainOperator;
    }

    @VisibleForTesting
    StreamTaskActionExecutor getActionExecutor() {
        return actionExecutor;
    }

    public ProcessingTimeServiceFactory getProcessingTimeServiceFactory() {
        return mailboxExecutor ->
                new ProcessingTimeServiceImpl(
                        timerService,
                        callback -> deferCallbackToMailbox(mailboxExecutor, callback));
    }

    /**
     * Handles an exception thrown by another thread (e.g. a TriggerTask), other than the one
     * executing the main task by failing the task entirely.
     *
     * <p>In more detail, it marks task execution failed for an external reason (a reason other than
     * the task code itself throwing an exception). If the task is already in a terminal state (such
     * as FINISHED, CANCELED, FAILED), or if the task is already canceling this does nothing.
     * Otherwise it sets the state to FAILED, and, if the invokable code is running, starts an
     * asynchronous thread that aborts that code.
     *
     * <p>This method never blocks.
     */
    @Override
    public void handleAsyncException(String message, Throwable exception) {
        if (isRunning) {
            // only fail if the task is still running
            asyncExceptionHandler.handleAsyncException(message, exception);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return getName();
    }

    // ------------------------------------------------------------------------

    /** Utility class to encapsulate the handling of asynchronous exceptions. */
    static class StreamTaskAsyncExceptionHandler {
        private final Environment environment;

        StreamTaskAsyncExceptionHandler(Environment environment) {
            this.environment = environment;
        }

        void handleAsyncException(String message, Throwable exception) {
            environment.failExternally(new AsynchronousException(message, exception));
        }
    }

    public final CloseableRegistry getCancelables() {
        return cancelables;
    }

    // ------------------------------------------------------------------------

    @VisibleForTesting
    public static <OUT>
            RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>>
                    createRecordWriterDelegate(
                            StreamConfig configuration, Environment environment) {
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWrites =
                createRecordWriters(configuration, environment);
        if (recordWrites.size() == 1) {
            return new SingleRecordWriter<>(recordWrites.get(0));
        } else if (recordWrites.size() == 0) {
            return new NonRecordWriter<>();
        } else {
            return new MultipleRecordWriters<>(recordWrites);
        }
    }

    private static <OUT>
            List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createRecordWriters(
                    StreamConfig configuration, Environment environment) {
        List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters =
                new ArrayList<>();
        List<StreamEdge> outEdgesInOrder =
                configuration.getOutEdgesInOrder(
                        environment.getUserCodeClassLoader().asClassLoader());

        for (int i = 0; i < outEdgesInOrder.size(); i++) {
            StreamEdge edge = outEdgesInOrder.get(i);
            recordWriters.add(
                    createRecordWriter(
                            edge,
                            i,
                            environment,
                            environment.getTaskInfo().getTaskName(),
                            edge.getBufferTimeout()));
        }
        return recordWriters;
    }

    @SuppressWarnings("unchecked")
    private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
            StreamEdge edge,
            int outputIndex,
            Environment environment,
            String taskName,
            long bufferTimeout) {

        StreamPartitioner<OUT> outputPartitioner = null;

        // Clones the partition to avoid multiple stream edges sharing the same stream partitioner,
        // like the case of https://issues.apache.org/jira/browse/FLINK-14087.
        try {
            outputPartitioner =
                    InstantiationUtil.clone(
                            (StreamPartitioner<OUT>) edge.getPartitioner(),
                            environment.getUserCodeClassLoader().asClassLoader());
        } catch (Exception e) {
            ExceptionUtils.rethrow(e);
        }

        LOG.debug(
                "Using partitioner {} for output {} of task {}",
                outputPartitioner,
                outputIndex,
                taskName);

        ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

        // we initialize the partitioner here with the number of key groups (aka max. parallelism)
        if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
            int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
            if (0 < numKeyGroups) {
                ((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
            }
        }

        RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output =
                new RecordWriterBuilder<SerializationDelegate<StreamRecord<OUT>>>()
                        .setChannelSelector(outputPartitioner)
                        .setTimeout(bufferTimeout)
                        .setTaskName(taskName)
                        .build(bufferWriter);
        output.setMetricGroup(environment.getMetricGroup().getIOMetricGroup());
        return output;
    }

    private void handleTimerException(Exception ex) {
        handleAsyncException("Caught exception while processing timer.", new TimerException(ex));
    }

    @VisibleForTesting
    ProcessingTimeCallback deferCallbackToMailbox(
            MailboxExecutor mailboxExecutor, ProcessingTimeCallback callback) {
        return timestamp -> {
            mailboxExecutor.execute(
                    () -> invokeProcessingTimeCallback(callback, timestamp),
                    "Timer callback for %s @ %d",
                    callback,
                    timestamp);
        };
    }

    private void invokeProcessingTimeCallback(ProcessingTimeCallback callback, long timestamp) {
        try {
            callback.onProcessingTime(timestamp);
        } catch (Throwable t) {
            handleAsyncException("Caught exception while processing timer.", new TimerException(t));
        }
    }

    protected long getAsyncCheckpointStartDelayNanos() {
        return latestAsyncCheckpointStartDelayNanos;
    }

    /**
     * Implementation of {@link org.apache.flink.streaming.runtime.tasks.mailbox.PeriodTimer} which
     * combine signal for metric and the throughput.
     */
    private static class ThroughputPeriodTimer implements PeriodTimer {
        private final Clock clock = SystemClock.getInstance();
        private final TimerGauge idleTimerGauge;
        private final ThroughputCalculator throughputCalculator;

        private ThroughputPeriodTimer(
                TimerGauge idleTimerGauge, ThroughputCalculator throughputCalculator) {
            this.idleTimerGauge = idleTimerGauge;
            this.throughputCalculator = throughputCalculator;
        }

        @Override
        public void markStart() {
            long absoluteTimeMillis = clock.absoluteTimeMillis();
            idleTimerGauge.markStart(absoluteTimeMillis);
            throughputCalculator.pauseMeasurement(absoluteTimeMillis);
        }

        @Override
        public void markEnd() {
            long absoluteTimeMillis = clock.absoluteTimeMillis();
            idleTimerGauge.markEnd(absoluteTimeMillis);
            throughputCalculator.resumeMeasurement(absoluteTimeMillis);
        }
    }

    @Override
    public boolean isUsingNonBlockingInput() {
        return true;
    }

    private void disableInterruptOnCancel() {
        synchronized (shouldInterruptOnCancelLock) {
            shouldInterruptOnCancel = false;
        }
    }

    @Override
    public void maybeInterruptOnCancel(
            Thread toInterrupt, @Nullable String taskName, @Nullable Long timeout) {
        synchronized (shouldInterruptOnCancelLock) {
            if (shouldInterruptOnCancel) {
                if (taskName != null && timeout != null) {
                    Task.logTaskThreadStackTrace(toInterrupt, taskName, timeout, "interrupting");
                }

                toInterrupt.interrupt();
            }
        }
    }

    @Override
    public final Environment getEnvironment() {
        return environment;
    }
}
