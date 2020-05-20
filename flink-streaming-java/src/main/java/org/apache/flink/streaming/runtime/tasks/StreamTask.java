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
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.writer.MultipleRecordWriters;
import org.apache.flink.runtime.io.network.api.writer.NonRecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterBuilder;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.SingleRecordWriter;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.CheckpointStorageWorkerView;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.runtime.io.RecordWriterOutput;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxExecutorFactory;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailbox;
import org.apache.flink.streaming.runtime.tasks.mailbox.TaskMailboxImpl;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed
 * and executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form
 * the Task's operator chain. Operators that are chained together execute synchronously in the
 * same thread and hence on the same stream partition. A common case for these chains
 * are successive map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators.
 * The StreamTask is specialized for the type of the head operator: one-input and two-input tasks,
 * as well as for sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * <p>The life cycle of the task is set up as follows:
 * <pre>{@code
 *  -- setInitialState -> provides state of all operators in the chain
 *
 *  -- invoke()
 *        |
 *        +----> Create basic utils (config, etc) and load the chain of operators
 *        +----> operators.setup()
 *        +----> task specific init()
 *        +----> initialize-operator-states()
 *        +----> open-operators()
 *        +----> run()
 *        +----> close-operators()
 *        +----> dispose-operators()
 *        +----> common cleanup
 *        +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a
 * {@code StreamOperator} must be synchronized on this lock object to ensure that no methods
 * are called concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
		extends AbstractInvokable
		implements AsyncExceptionHandler {

	/** The thread group that holds all trigger timer threads. */
	public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

	/** The logger used by the StreamTask and its subclasses. */
	protected static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	// ------------------------------------------------------------------------

	/**
	 * All actions outside of the task {@link #mailboxProcessor mailbox} (i.e. performed by another thread) must be executed through this executor
	 * to ensure that we don't have concurrent method calls that void consistent checkpoints.
	 * <p>CheckpointLock is superseded by {@link MailboxExecutor}, with
	 * {@link StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor SynchronizedStreamTaskActionExecutor}
	 * to provide lock to {@link SourceStreamTask}. </p>
	 */
	private final StreamTaskActionExecutor actionExecutor;

	/**
	 * The input processor. Initialized in {@link #init()} method.
	 */
	@Nullable
	protected StreamInputProcessor inputProcessor;

	/** the head operator that consumes the input streams of this task. */
	protected OP headOperator;

	/** The chain of operators executed by this task. */
	protected OperatorChain<OUT, OP> operatorChain;

	/** The configuration of this streaming task. */
	protected final StreamConfig configuration;

	/** Our state backend. We use this to create checkpoint streams and a keyed state backend. */
	protected final StateBackend stateBackend;

	private final SubtaskCheckpointCoordinator subtaskCheckpointCoordinator;

	/**
	 * The internal {@link TimerService} used to define the current
	 * processing time (default = {@code System.currentTimeMillis()}) and
	 * register timers for tasks to be executed in the future.
	 */
	protected final TimerService timerService;

	/** The currently active background materialization threads. */
	private final CloseableRegistry cancelables = new CloseableRegistry();

	private final StreamTaskAsyncExceptionHandler asyncExceptionHandler;

	/**
	 * Flag to mark the task "in operation", in which case check needs to be initialized to true,
	 * so that early cancel() before invoke() behaves correctly.
	 */
	private volatile boolean isRunning;

	/** Flag to mark this task as canceled. */
	private volatile boolean canceled;

	private boolean disposedOperators;

	/** Thread pool for async snapshot workers. */
	private final ExecutorService asyncOperationsThreadPool;

	private final RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriter;

	protected final MailboxProcessor mailboxProcessor;

	/**
	 * TODO it might be replaced by the global IO executor on TaskManager level future.
	 */
	private final ExecutorService channelIOExecutor;

	private Long syncSavepointId = null;

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
			Thread.UncaughtExceptionHandler uncaughtExceptionHandler) throws Exception {
		this(environment, timerService, uncaughtExceptionHandler, StreamTaskActionExecutor.IMMEDIATE);
	}

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * <p>This constructor accepts a special {@link TimerService}. By default (and if
	 * null is passes for the timer service) a {@link SystemProcessingTimeService DefaultTimerService}
	 * will be used.
	 *
	 * @param environment The task environment for this task.
	 * @param timerService Optionally, a specific timer service to use.
	 * @param uncaughtExceptionHandler to handle uncaught exceptions in the async operations thread pool
	 * @param actionExecutor a mean to wrap all actions performed by this task thread. Currently, only SynchronizedActionExecutor can be used to preserve locking semantics.
	 */
	protected StreamTask(
			Environment environment,
			@Nullable TimerService timerService,
			Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
			StreamTaskActionExecutor actionExecutor) throws Exception {
		this(environment, timerService, uncaughtExceptionHandler, actionExecutor, new TaskMailboxImpl(Thread.currentThread()));
	}

	protected StreamTask(
			Environment environment,
			@Nullable TimerService timerService,
			Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
			StreamTaskActionExecutor actionExecutor,
			TaskMailbox mailbox) throws Exception {

		super(environment);

		this.configuration = new StreamConfig(getTaskConfiguration());
		this.recordWriter = createRecordWriterDelegate(configuration, environment);
		this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
		this.mailboxProcessor = new MailboxProcessor(this::processInput, mailbox, actionExecutor);
		this.mailboxProcessor.initMetric(environment.getMetricGroup());
		this.asyncExceptionHandler = new StreamTaskAsyncExceptionHandler(environment);
		this.asyncOperationsThreadPool = Executors.newCachedThreadPool(
			new ExecutorThreadFactory("AsyncOperations", uncaughtExceptionHandler));

		this.stateBackend = createStateBackend();

		this.subtaskCheckpointCoordinator = new SubtaskCheckpointCoordinatorImpl(
			stateBackend.createCheckpointStorage(getEnvironment().getJobID()),
			getName(),
			actionExecutor,
			getCancelables(),
			getAsyncOperationsThreadPool(),
			getEnvironment(),
			this,
			configuration.isUnalignedCheckpointsEnabled(),
			this::prepareInputSnapshot);

		// if the clock is not already set, then assign a default TimeServiceProvider
		if (timerService == null) {
			ThreadFactory timerThreadFactory = new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, "Time Trigger for " + getName());
			this.timerService = new SystemProcessingTimeService(this::handleTimerException, timerThreadFactory);
		} else {
			this.timerService = timerService;
		}

		this.channelIOExecutor = Executors.newSingleThreadExecutor(new ExecutorThreadFactory("channel-state-unspilling"));
	}

	private CompletableFuture<Void> prepareInputSnapshot(ChannelStateWriter channelStateWriter, long checkpointId) throws IOException {
		if (inputProcessor == null) {
			return FutureUtils.completedVoidFuture();
		}
		return inputProcessor.prepareSnapshot(channelStateWriter, checkpointId);
	}

	protected ChannelStateWriter getChannelStateWriter() {
		return subtaskCheckpointCoordinator.getChannelStateWriter();
	}

	// ------------------------------------------------------------------------
	//  Life cycle methods for specific implementations
	// ------------------------------------------------------------------------

	protected abstract void init() throws Exception;

	protected void cancelTask() throws Exception {
	}

	protected void cleanup() throws Exception {
		if (inputProcessor != null) {
			inputProcessor.close();
		}
	}

	/**
	 * This method implements the default action of the task (e.g. processing one event from the input). Implementations
	 * should (in general) be non-blocking.
	 *
	 * @param controller controller object for collaborative interaction between the action and the stream task.
	 * @throws Exception on any problems in the action.
	 */
	protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
		InputStatus status = inputProcessor.processInput();
		if (status == InputStatus.MORE_AVAILABLE && recordWriter.isAvailable()) {
			return;
		}
		if (status == InputStatus.END_OF_INPUT) {
			controller.allActionsCompleted();
			return;
		}
		CompletableFuture<?> jointFuture = getInputOutputJointFuture(status);
		MailboxDefaultAction.Suspension suspendedDefaultAction = controller.suspendDefaultAction();
		jointFuture.thenRun(suspendedDefaultAction::resume);
	}

	/**
	 * Considers three scenarios to combine input and output futures:
	 * 1. Both input and output are unavailable.
	 * 2. Only input is unavailable.
	 * 3. Only output is unavailable.
	 */
	@VisibleForTesting
	CompletableFuture<?> getInputOutputJointFuture(InputStatus status) {
		if (status == InputStatus.NOTHING_AVAILABLE && !recordWriter.isAvailable()) {
			return CompletableFuture.allOf(inputProcessor.getAvailableFuture(), recordWriter.getAvailableFuture());
		} else if (status == InputStatus.NOTHING_AVAILABLE) {
			return inputProcessor.getAvailableFuture();
		} else {
			return recordWriter.getAvailableFuture();
		}
	}

	private void resetSynchronousSavepointId() {
		syncSavepointId = null;
	}

	private void setSynchronousSavepointId(long checkpointId) {
		Preconditions.checkState(
			syncSavepointId == null, "at most one stop-with-savepoint checkpoint at a time is allowed");
		syncSavepointId = checkpointId;
	}

	@VisibleForTesting
	OptionalLong getSynchronousSavepointId() {
		return syncSavepointId != null ? OptionalLong.of(syncSavepointId) : OptionalLong.empty();
	}

	private boolean isSynchronousSavepointId(long checkpointId) {
		return syncSavepointId != null && syncSavepointId == checkpointId;
	}

	private void runSynchronousSavepointMailboxLoop() throws Exception {
		assert syncSavepointId != null;

		MailboxExecutor mailboxExecutor = mailboxProcessor.getMailboxExecutor(TaskMailbox.MAX_PRIORITY);

		while (!canceled && syncSavepointId != null) {
			mailboxExecutor.yield();
		}
	}

	/**
	 * Emits the {@link org.apache.flink.streaming.api.watermark.Watermark#MAX_WATERMARK MAX_WATERMARK}
	 * so that all registered timers are fired.
	 *
	 * <p>This is used by the source task when the job is {@code TERMINATED}. In the case,
	 * we want all the timers registered throughout the pipeline to fire and the related
	 * state (e.g. windows) to be flushed.
	 *
	 * <p>For tasks other than the source task, this method does nothing.
	 */
	protected void advanceToEndOfEventTime() throws Exception {

	}

	/**
	 * Instructs the task to go through its normal termination routine, i.e. exit the run-loop
	 * and call {@link StreamOperator#close()} and {@link StreamOperator#dispose()} on its operators.
	 *
	 * <p>This is used by the source task to get out of the run-loop when the job is stopped with a savepoint.
	 *
	 * <p>For tasks other than the source task, this method does nothing.
	 */
	protected void finishTask() throws Exception {

	}

	// ------------------------------------------------------------------------
	//  Core work methods of the Stream Task
	// ------------------------------------------------------------------------

	public StreamTaskStateInitializer createStreamTaskStateInitializer() {
		return new StreamTaskStateInitializerImpl(
			getEnvironment(),
			stateBackend);
	}

	protected Counter setupNumRecordsInCounter(StreamOperator streamOperator) {
		try {
			return ((OperatorMetricGroup) streamOperator.getMetricGroup()).getIOMetricGroup().getNumRecordsInCounter();
		} catch (Exception e) {
			LOG.warn("An exception occurred during the metrics setup.", e);
			return new SimpleCounter();
		}
	}

	protected void beforeInvoke() throws Exception {
		disposedOperators = false;
		LOG.debug("Initializing {}.", getName());

		operatorChain = new OperatorChain<>(this, recordWriter);
		headOperator = operatorChain.getHeadOperator();

		// task specific initialization
		init();

		// save the work of reloading state, etc, if the task is already canceled
		if (canceled) {
			throw new CancelTaskException();
		}

		// -------- Invoke --------
		LOG.debug("Invoking {}", getName());

		// we need to make sure that any triggers scheduled in open() cannot be
		// executed before all operators are opened
		actionExecutor.runThrowing(() -> {
			// both the following operations are protected by the lock
			// so that we avoid race conditions in the case that initializeState()
			// registers a timer, that fires before the open() is called.
			operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());

			readRecoveredChannelState();
		});

		isRunning = true;
	}

	private void readRecoveredChannelState() throws IOException, InterruptedException {
		ChannelStateReader reader = getEnvironment().getTaskStateManager().getChannelStateReader();
		if (!reader.hasChannelStates()) {
			requestPartitions();
			return;
		}

		ResultPartitionWriter[] writers = getEnvironment().getAllWriters();
		if (writers != null) {
			for (ResultPartitionWriter writer : writers) {
				writer.readRecoveredState(reader);
			}
		}

		// It would get possible benefits to recovery input side after output side, which guarantees the
		// output can request more floating buffers from global firstly.
		InputGate[] inputGates = getEnvironment().getAllInputGates();
		if (inputGates != null && inputGates.length > 0) {
			CompletableFuture[] futures = new CompletableFuture[inputGates.length];
			for (int i = 0; i < inputGates.length; i++) {
				futures[i] = inputGates[i].readRecoveredState(channelIOExecutor, reader);
			}

			// Note that we must request partition after all the single gates finished recovery.
			CompletableFuture.allOf(futures).thenRun(() -> mailboxProcessor.getMainMailboxExecutor().execute(
				this::requestPartitions, "Input gates request partitions"));
		}
	}

	private void requestPartitions() throws IOException {
		InputGate[] inputGates = getEnvironment().getAllInputGates();
		if (inputGates != null) {
			for (InputGate inputGate : inputGates) {
				inputGate.requestPartitions();
			}
		}
	}

	@Override
	public final void invoke() throws Exception {
		try {
			beforeInvoke();

			// final check to exit early before starting to run
			if (canceled) {
				throw new CancelTaskException();
			}

			// let the task do its work
			runMailboxLoop();

			// if this left the run() method cleanly despite the fact that this was canceled,
			// make sure the "clean shutdown" is not attempted
			if (canceled) {
				throw new CancelTaskException();
			}

			afterInvoke();
		}
		catch (Exception invokeException) {
			try {
				cleanUpInvoke();
			}
			catch (Throwable cleanUpException) {
				throw (Exception) ExceptionUtils.firstOrSuppressed(cleanUpException, invokeException);
			}
			throw invokeException;
		}
		cleanUpInvoke();
	}

	protected boolean runMailboxStep() throws Exception {
		return mailboxProcessor.runMailboxStep();
	}

	private void runMailboxLoop() throws Exception {
		mailboxProcessor.runMailboxLoop();
	}

	protected void afterInvoke() throws Exception {
		LOG.debug("Finished task {}", getName());

		final CompletableFuture<Void> timersFinishedFuture = new CompletableFuture<>();

		// close all operators in a chain effect way
		operatorChain.closeOperators(actionExecutor);

		// make sure no further checkpoint and notification actions happen.
		// at the same time, this makes sure that during any "regular" exit where still
		actionExecutor.runThrowing(() -> {

			// make sure no new timers can come
			FutureUtils.forward(timerService.quiesce(), timersFinishedFuture);

			// let mailbox execution reject all new letters from this point
			mailboxProcessor.prepareClose();

			// only set the StreamTask to not running after all operators have been closed!
			// See FLINK-7430
			isRunning = false;
		});
		// processes the remaining mails; no new mails can be enqueued
		mailboxProcessor.drain();

		// make sure all timers finish
		timersFinishedFuture.get();

		LOG.debug("Closed operators for task {}", getName());

		// make sure all buffered data is flushed
		operatorChain.flushOutputs();

		// make an attempt to dispose the operators such that failures in the dispose call
		// still let the computation fail
		disposeAllOperators(false);
		disposedOperators = true;
	}

	protected void cleanUpInvoke() throws Exception {
		// clean up everything we initialized
		isRunning = false;

		// Now that we are outside the user code, we do not want to be interrupted further
		// upon cancellation. The shutdown logic below needs to make sure it does not issue calls
		// that block and stall shutdown.
		// Additionally, the cancellation watch dog will issue a hard-cancel (kill the TaskManager
		// process) as a backup in case some shutdown procedure blocks outside our control.
		setShouldInterruptOnCancel(false);

		// clear any previously issued interrupt for a more graceful shutdown
		Thread.interrupted();

		// stop all timers and threads
		tryShutdownTimerService();

		// stop all asynchronous checkpoint threads
		try {
			cancelables.close();
			shutdownAsyncThreads();
		} catch (Throwable t) {
			// catch and log the exception to not replace the original exception
			LOG.error("Could not shut down async checkpoint threads", t);
		}

		// we must! perform this cleanup
		try {
			cleanup();
		} catch (Throwable t) {
			// catch and log the exception to not replace the original exception
			LOG.error("Error during cleanup of stream task", t);
		}

		// if the operators were not disposed before, do a hard dispose
		disposeAllOperators(true);

		// release the output resources. this method should never fail.
		if (operatorChain != null) {
			// beware: without synchronization, #performCheckpoint() may run in
			//         parallel and this call is not thread-safe
			actionExecutor.run(() -> operatorChain.releaseOutputs());
		} else {
			// failed to allocate operatorChain, clean up record writers
			recordWriter.close();
		}

		try {
			channelIOExecutor.shutdown();
		} catch (Throwable t) {
			LOG.error("Error during shutdown the channel state unspill executor", t);
		}

		mailboxProcessor.close();
	}

	@Override
	public final void cancel() throws Exception {
		isRunning = false;
		canceled = true;

		// the "cancel task" call must come first, but the cancelables must be
		// closed no matter what
		try {
			cancelTask();
		}
		finally {
			mailboxProcessor.allActionsCompleted();
			cancelables.close();
		}
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

	private void shutdownAsyncThreads() throws Exception {
		if (!asyncOperationsThreadPool.isShutdown()) {
			asyncOperationsThreadPool.shutdownNow();
		}
	}

	/**
	 * Execute @link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 */
	private void disposeAllOperators(boolean logOnlyErrors) throws Exception {
		if (operatorChain != null && !disposedOperators) {
			for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperators(true)) {
				StreamOperator<?> operator = operatorWrapper.getStreamOperator();
				if (!logOnlyErrors) {
					operator.dispose();
				}
				else {
					try {
						operator.dispose();
					}
					catch (Exception e) {
						LOG.error("Error during disposal of stream operator.", e);
					}
				}
			}
			disposedOperators = true;
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
		return getEnvironment().getTaskInfo().getTaskNameWithSubtasks() +
			" (" + getEnvironment().getExecutionId() + ')';
	}

	public CheckpointStorageWorkerView getCheckpointStorage() {
		return subtaskCheckpointCoordinator.getCheckpointStorage();
	}

	public StreamConfig getConfiguration() {
		return configuration;
	}

	public StreamStatusMaintainer getStreamStatusMaintainer() {
		return operatorChain;
	}

	RecordWriterOutput<?>[] getStreamOutputs() {
		return operatorChain.getStreamOutputs();
	}

	// ------------------------------------------------------------------------
	//  Checkpoint and Restore
	// ------------------------------------------------------------------------

	@Override
	public Future<Boolean> triggerCheckpointAsync(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			boolean advanceToEndOfEventTime) {

		CompletableFuture<Boolean> result = new CompletableFuture<>();
		mailboxProcessor.getMainMailboxExecutor().execute(
				() -> {
					try {
						result.complete(triggerCheckpoint(checkpointMetaData, checkpointOptions, advanceToEndOfEventTime));
					}
					catch (Exception ex) {
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

	private boolean triggerCheckpoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			boolean advanceToEndOfEventTime) throws Exception {
		try {
			// No alignment if we inject a checkpoint
			CheckpointMetrics checkpointMetrics = new CheckpointMetrics().setAlignmentDurationNanos(0L);

			subtaskCheckpointCoordinator.getChannelStateWriter().start(checkpointMetaData.getCheckpointId(), checkpointOptions);
			boolean success = performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics, advanceToEndOfEventTime);
			if (!success) {
				declineCheckpoint(checkpointMetaData.getCheckpointId());
			}
			return success;
		} catch (Exception e) {
			// propagate exceptions only if the task is still in "running" state
			if (isRunning) {
				throw new Exception("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() +
					" for operator " + getName() + '.', e);
			} else {
				LOG.debug("Could not perform checkpoint {} for operator {} while the " +
					"invokable was not in state running.", checkpointMetaData.getCheckpointId(), getName(), e);
				return false;
			}
		}
	}

	@Override
	public <E extends Exception> void executeInTaskThread(
			ThrowingRunnable<E> runnable,
			String descriptionFormat,
			Object... descriptionArgs) throws E {
		if (mailboxProcessor.isMailboxThread()) {
			runnable.run();
		} else {
			mailboxProcessor.getMainMailboxExecutor().execute(runnable, descriptionFormat, descriptionArgs);
		}
	}

	@Override
	public void triggerCheckpointOnBarrier(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics) throws IOException {

		try {
			if (performCheckpoint(checkpointMetaData, checkpointOptions, checkpointMetrics, false)) {
				if (isSynchronousSavepointId(checkpointMetaData.getCheckpointId())) {
					runSynchronousSavepointMailboxLoop();
				}
			}
		}
		catch (CancelTaskException e) {
			LOG.info("Operator {} was cancelled while performing checkpoint {}.",
					getName(), checkpointMetaData.getCheckpointId());
			throw e;
		}
		catch (Exception e) {
			throw new IOException("Could not perform checkpoint " + checkpointMetaData.getCheckpointId() + " for operator " +
				getName() + '.', e);
		}
	}

	@Override
	public void abortCheckpointOnBarrier(long checkpointId, Throwable cause) throws IOException {
		subtaskCheckpointCoordinator.abortCheckpointOnBarrier(checkpointId, cause, operatorChain);
	}

	private boolean performCheckpoint(
			CheckpointMetaData checkpointMetaData,
			CheckpointOptions checkpointOptions,
			CheckpointMetrics checkpointMetrics,
			boolean advanceToEndOfTime) throws Exception {

		LOG.debug("Starting checkpoint ({}) {} on task {}",
			checkpointMetaData.getCheckpointId(), checkpointOptions.getCheckpointType(), getName());

		if (isRunning) {
			actionExecutor.runThrowing(() -> {

				if (checkpointOptions.getCheckpointType().isSynchronous()) {
					setSynchronousSavepointId(checkpointMetaData.getCheckpointId());

					if (advanceToEndOfTime) {
						advanceToEndOfEventTime();
					}
				}

				subtaskCheckpointCoordinator.checkpointState(
					checkpointMetaData,
					checkpointOptions,
					checkpointMetrics,
					operatorChain,
					this::isCanceled);
			});

			return true;
		} else {
			actionExecutor.runThrowing(() -> {
				// we cannot perform our checkpoint - let the downstream operators know that they
				// should not wait for any input from this operator

				// we cannot broadcast the cancellation markers on the 'operator chain', because it may not
				// yet be created
				final CancelCheckpointMarker message = new CancelCheckpointMarker(checkpointMetaData.getCheckpointId());
				recordWriter.broadcastEvent(message);
			});

			return false;
		}
	}

	protected void declineCheckpoint(long checkpointId) {
		getEnvironment().declineCheckpoint(
			checkpointId,
			new CheckpointException("Task Name" + getName(), CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY));
	}

	public final ExecutorService getAsyncOperationsThreadPool() {
		return asyncOperationsThreadPool;
	}

	@Override
	public Future<Void> notifyCheckpointCompleteAsync(long checkpointId) {
		return mailboxProcessor.getMailboxExecutor(TaskMailbox.MAX_PRIORITY).submit(
				() -> notifyCheckpointComplete(checkpointId),
				"checkpoint %d complete", checkpointId);
	}

	private void notifyCheckpointComplete(long checkpointId) throws Exception {
		subtaskCheckpointCoordinator.notifyCheckpointComplete(checkpointId, operatorChain, this::isRunning);
		if (isRunning && isSynchronousSavepointId(checkpointId)) {
			finishTask();
			// Reset to "notify" the internal synchronous savepoint mailbox loop.
			resetSynchronousSavepointId();
		}
	}

	@Override
	public Future<Void> notifyCheckpointAbortAsync(long checkpointId) {
		return mailboxProcessor.getMailboxExecutor(TaskMailbox.MAX_PRIORITY).submit(
			() -> subtaskCheckpointCoordinator.notifyCheckpointAborted(checkpointId, operatorChain, this::isRunning),
			"checkpoint %d aborted", checkpointId);
	}

	private void tryShutdownTimerService() {

		if (!timerService.isTerminated()) {

			try {
				final long timeoutMs = getEnvironment().getTaskManagerInfo().getConfiguration().
					getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT_TIMERS);

				if (!timerService.shutdownServiceUninterruptible(timeoutMs)) {
					LOG.warn("Timer service shutdown exceeded time limit of {} ms while waiting for pending " +
						"timers. Will continue with shutdown procedure.", timeoutMs);
				}
			} catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down timer service", t);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Operator Events
	// ------------------------------------------------------------------------

	@Override
	public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event) throws FlinkException {
		try {
			mailboxProcessor.getMainMailboxExecutor().execute(
				() -> {
					try {
						operatorChain.dispatchOperatorEvent(operator, event);
					} catch (Throwable t) {
						mailboxProcessor.reportThrowable(t);
					}
				},
				"dispatch operator event");
		}
		catch (RejectedExecutionException e) {
			// this happens during shutdown, we can swallow this
		}
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	private StateBackend createStateBackend() throws Exception {
		final StateBackend fromApplication = configuration.getStateBackend(getUserCodeClassLoader());

		return StateBackendLoader.fromApplicationOrConfigOrDefault(
				fromApplication,
				getEnvironment().getTaskManagerInfo().getConfiguration(),
				getUserCodeClassLoader(),
				LOG);
	}

	/**
	 * Returns the {@link TimerService} responsible for telling the current processing time and registering actual timers.
	 */
	@VisibleForTesting
	TimerService getTimerService() {
		return timerService;
	}

	@VisibleForTesting
	OP getHeadOperator() {
		return this.headOperator;
	}

	@VisibleForTesting
	StreamTaskActionExecutor getActionExecutor() {
		return actionExecutor;
	}

	public ProcessingTimeServiceFactory getProcessingTimeServiceFactory() {
		return mailboxExecutor -> new ProcessingTimeServiceImpl(
			timerService,
			callback -> deferCallbackToMailbox(mailboxExecutor, callback));
	}

	/**
	 * Handles an exception thrown by another thread (e.g. a TriggerTask),
	 * other than the one executing the main task by failing the task entirely.
	 *
	 * <p>In more detail, it marks task execution failed for an external reason
	 * (a reason other than the task code itself throwing an exception). If the task
	 * is already in a terminal state (such as FINISHED, CANCELED, FAILED), or if the
	 * task is already canceling this does nothing. Otherwise it sets the state to
	 * FAILED, and, if the invokable code is running, starts an asynchronous thread
	 * that aborts that code.
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

	/**
	 * Utility class to encapsulate the handling of asynchronous exceptions.
	 */
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
	public static <OUT> RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> createRecordWriterDelegate(
			StreamConfig configuration,
			Environment environment) {
		List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWrites = createRecordWriters(
			configuration,
			environment);
		if (recordWrites.size() == 1) {
			return new SingleRecordWriter<>(recordWrites.get(0));
		} else if (recordWrites.size() == 0) {
			return new NonRecordWriter<>();
		} else {
			return new MultipleRecordWriters<>(recordWrites);
		}
	}

	private static <OUT> List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createRecordWriters(
			StreamConfig configuration,
			Environment environment) {
		List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters = new ArrayList<>();
		List<StreamEdge> outEdgesInOrder = configuration.getOutEdgesInOrder(environment.getUserClassLoader());
		Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigsWithSelf(environment.getUserClassLoader());

		for (int i = 0; i < outEdgesInOrder.size(); i++) {
			StreamEdge edge = outEdgesInOrder.get(i);
			recordWriters.add(
				createRecordWriter(
					edge,
					i,
					environment,
					environment.getTaskInfo().getTaskName(),
					chainedConfigs.get(edge.getSourceId()).getBufferTimeout()));
		}
		return recordWriters;
	}

	private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
			StreamEdge edge,
			int outputIndex,
			Environment environment,
			String taskName,
			long bufferTimeout) {
		@SuppressWarnings("unchecked")
		StreamPartitioner<OUT> outputPartitioner = (StreamPartitioner<OUT>) edge.getPartitioner();

		LOG.debug("Using partitioner {} for output {} of task {}", outputPartitioner, outputIndex, taskName);

		ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

		// we initialize the partitioner here with the number of key groups (aka max. parallelism)
		if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
			int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
			if (0 < numKeyGroups) {
				((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
			}
		}

		RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output = new RecordWriterBuilder<SerializationDelegate<StreamRecord<OUT>>>()
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
	ProcessingTimeCallback deferCallbackToMailbox(MailboxExecutor mailboxExecutor, ProcessingTimeCallback callback) {
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
}
