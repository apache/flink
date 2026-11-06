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

package org.apache.flink.streaming.api.operators.python.embedded;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.util.ProtoUtils;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.python.DataStreamPythonFunctionInfo;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.SupportsChainAvailability;
import org.apache.flink.streaming.api.operators.async.queue.OrderedStreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.StreamElementQueue;
import org.apache.flink.streaming.api.operators.async.queue.UnorderedStreamElementQueue;
import org.apache.flink.streaming.api.utils.PythonTypeUtils;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.AbstractMessageLite;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.flink.python.PythonOptions.MAP_STATE_READ_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.MAP_STATE_WRITE_CACHE_SIZE;
import static org.apache.flink.python.PythonOptions.PYTHON_METRIC_ENABLED;
import static org.apache.flink.python.PythonOptions.PYTHON_PROFILE_ENABLED;
import static org.apache.flink.python.PythonOptions.STATE_CACHE_SIZE;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;

/**
 * Executes a Python {@code AsyncFunction} in thread mode. The queue, timeouts, checkpointing and
 * emission follow {@link org.apache.flink.streaming.api.operators.async.AsyncWaitOperator}. Python
 * runs {@code async_invoke} on an asyncio event loop thread and completes the queue entries through
 * a {@link Handler}, whose results are processed in the mailbox.
 */
@Internal
public class EmbeddedPythonAsyncWaitOperator<IN, OUT>
        extends AbstractOneInputEmbeddedPythonFunctionOperator<IN, OUT>
        implements SupportsChainAvailability {

    private static final long serialVersionUID = 1L;

    private static final String STATE_NAME = "_async_wait_operator_state_";

    /** Capacity of the stream element queue. */
    private final int capacity;

    /** Timeout in milliseconds, or 0 for no timeout. */
    private final long timeout;

    /** Whether the results are emitted in the order of the inputs. */
    private final boolean ordered;

    private transient MailboxExecutor mailboxExecutor;

    /** Queue, into which to store the currently in-flight stream elements. */
    private transient StreamElementQueue<OUT> queue;

    private transient StreamElementSerializer<IN> inStreamElementSerializer;

    private transient TypeSerializer<OUT> outSerializer;

    private transient ListState<StreamElement> recoveredStreamElements;

    private transient PythonTypeUtils.DataConverter<IN, Object> inputConverter;

    private transient AvailabilityProvider downstreamAvailabilityProvider;

    /** Set when the operator starts closing; late results and timeouts are dropped after that. */
    private transient volatile boolean closing;

    private transient boolean pythonOperationOpened;

    private transient boolean pythonOperationClosed;

    public EmbeddedPythonAsyncWaitOperator(
            Configuration config,
            DataStreamPythonFunctionInfo pythonFunctionInfo,
            TypeInformation<IN> inputTypeInfo,
            TypeInformation<OUT> outputTypeInfo,
            int capacity,
            long timeout,
            boolean ordered) {
        super(config, pythonFunctionInfo, inputTypeInfo, outputTypeInfo);
        Preconditions.checkArgument(
                capacity > 0, "The number of concurrent async operation should be greater than 0.");
        Preconditions.checkArgument(timeout >= 0L, "The timeout should not be negative.");
        this.capacity = capacity;
        this.timeout = timeout;
        this.ordered = ordered;
    }

    @Override
    public void setMailboxExecutor(MailboxExecutor mailboxExecutor) {
        super.setMailboxExecutor(mailboxExecutor);
        this.mailboxExecutor = mailboxExecutor;
    }

    @Override
    protected void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<OUT>> output) {
        super.setup(containingTask, config, output);
        this.inStreamElementSerializer =
                new StreamElementSerializer<>(
                        getOperatorConfig().<IN>getTypeSerializerIn1(getUserCodeClassloader()));
        this.queue =
                ordered
                        ? new OrderedStreamElementQueue<>(capacity)
                        : new UnorderedStreamElementQueue<>(capacity);
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        recoveredStreamElements =
                context.getOperatorStateStore()
                        .getListState(
                                new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        ListState<StreamElement> partitionableState =
                getOperatorStateBackend()
                        .getListState(
                                new ListStateDescriptor<>(STATE_NAME, inStreamElementSerializer));
        try {
            partitionableState.update(queue.values());
        } catch (Exception e) {
            partitionableState.clear();
            throw new Exception(
                    "Could not add stream element queue entries to operator state "
                            + "backend of operator "
                            + getOperatorName()
                            + '.',
                    e);
        }
    }

    @Override
    public void open() throws Exception {
        super.open();
        openAsyncWait();
    }

    /** Prepares the conversions and replays the recovered elements once Python is ready. */
    @VisibleForTesting
    void openAsyncWait() throws Exception {
        closing = false;
        outSerializer = outputTypeInfo.createSerializer(getExecutionConfig().getSerializerConfig());
        inputConverter =
                PythonTypeUtils.TypeInfoToDataConverter.typeInfoDataConverter(getInputTypeInfo());
        if (recoveredStreamElements != null) {
            for (StreamElement element : recoveredStreamElements.get()) {
                if (element.isRecord()) {
                    processElement(element.<IN>asRecord());
                } else if (element.isWatermark()) {
                    processWatermark(element.asWatermark());
                } else {
                    throw new IllegalStateException(
                            "Unknown record type "
                                    + element.getClass()
                                    + " encountered while opening the operator.");
                }
            }
            recoveredStreamElements = null;
        }
    }

    @Override
    public void openPythonInterpreter() {
        interpreter.set(
                "function_protos",
                createUserDefinedFunctionsProto().stream()
                        .map(AbstractMessageLite::toByteArray)
                        .collect(Collectors.toList()));
        interpreter.set(
                "input_coder_info",
                ProtoUtils.createRawTypeCoderInfoDescriptorProto(
                                getInputTypeInfo(),
                                FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE,
                                false,
                                null)
                        .toByteArray());
        interpreter.set(
                "output_coder_info",
                ProtoUtils.createRawTypeCoderInfoDescriptorProto(
                                outputTypeInfo,
                                FlinkFnApi.CoderInfoDescriptor.Mode.SINGLE,
                                false,
                                null)
                        .toByteArray());
        interpreter.set("runtime_context", getRuntimeContext());
        interpreter.set("job_parameters", getJobParameters());
        interpreter.exec(
                "from pyflink.fn_execution.embedded.async_operation import create_async_operation_from_protos");
        interpreter.exec(
                "operation = create_async_operation_from_protos("
                        + "function_protos, input_coder_info, output_coder_info,"
                        + " runtime_context, job_parameters)");
        interpreter.invokeMethod("operation", "open");
        pythonOperationOpened = true;
    }

    @Override
    public void processElement(StreamRecord<IN> record) throws Exception {
        StreamRecord<IN> element;
        // copy the element to avoid that it is reused
        if (getExecutionConfig().isObjectReuseEnabled()) {
            //noinspection unchecked
            element = (StreamRecord<IN>) inStreamElementSerializer.copy(record);
        } else {
            element = record;
        }

        final ResultFuture<OUT> entry = addToWorkQueue(element);

        final Handler handler = new Handler(element, entry);

        if (timeout > 0L) {
            long timeoutTimestamp = getProcessingTimeService().getCurrentProcessingTime() + timeout;
            handler.timeoutTimer =
                    getProcessingTimeService()
                            .registerTimer(timeoutTimestamp, timestamp -> handler.onTimeout());
        }

        invokeAsync(handler, inputConverter.toExternal(element.getValue()));
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        addToWorkQueue(mark);

        // watermarks are always completed, so emit them directly if there is no prior element
        outputCompletedElement();
    }

    @Override
    public void endInput() {
        // wait for the in-flight inputs here, because timers no longer fire after this
        try {
            while (!queue.isEmpty()) {
                mailboxExecutor.yield();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        closePythonOperation();
    }

    @Override
    public void close() throws Exception {
        closing = true;
        try {
            closePythonOperation();
        } finally {
            super.close();
        }
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return queue.getAvailableFuture();
    }

    @Override
    public void setDownstreamAvailabilityProvider(AvailabilityProvider provider) {
        this.downstreamAvailabilityProvider = provider;
    }

    /** Starts {@code async_invoke} for the given input on the Python event loop thread. */
    @VisibleForTesting
    void invokeAsync(Handler handler, Object value) {
        interpreter.invokeMethod("operation", "async_invoke", handler, value);
    }

    /** Calls the Python {@code timeout} for the given input on the task thread. */
    @VisibleForTesting
    void invokeTimeout(Handler handler, Object value) {
        interpreter.invokeMethod("operation", "timeout", handler, value);
    }

    /** Stops the event loop thread and closes the Python function, at most once. */
    @VisibleForTesting
    void closePythonOperation() {
        if (pythonOperationOpened && !pythonOperationClosed) {
            pythonOperationClosed = true;
            interpreter.invokeMethod("operation", "close");
        }
    }

    private ResultFuture<OUT> addToWorkQueue(StreamElement streamElement)
            throws InterruptedException {
        Optional<ResultFuture<OUT>> queueEntry;
        while (!(queueEntry = queue.tryPut(streamElement)).isPresent()) {
            mailboxExecutor.yield();
        }
        return queueEntry.get();
    }

    private void outputCompletedElement() {
        if (queue.hasCompletedElements()) {
            // defer emission until downstream becomes available
            if (downstreamAvailabilityProvider != null
                    && !downstreamAvailabilityProvider.isAvailable()) {
                downstreamAvailabilityProvider
                        .getAvailableFuture()
                        .thenRun(
                                () -> {
                                    try {
                                        mailboxExecutor.execute(
                                                this::outputCompletedElement,
                                                "EmbeddedPythonAsyncWaitOperator#outputCompletedElement(deferred)");
                                    } catch (RejectedExecutionException e) {
                                        LOG.debug(
                                                "Deferred element emission is ignored since the mailbox rejected the execution.",
                                                e);
                                    }
                                });
                return;
            }
            // emit only one element to not block the mailbox thread unnecessarily
            queue.emitCompletedElement(collector);
            // if there are more completed elements, emit them with subsequent mails
            if (queue.hasCompletedElements()) {
                try {
                    mailboxExecutor.execute(
                            this::outputCompletedElement,
                            "EmbeddedPythonAsyncWaitOperator#outputCompletedElement");
                } catch (RejectedExecutionException e) {
                    // the task is shutting down; the pending records are replayed after recovery
                    LOG.debug(
                            "Attempt to complete element is ignored since the mailbox rejected the execution.",
                            e);
                }
            }
        }
    }

    @Override
    public List<FlinkFnApi.UserDefinedDataStreamFunction> createUserDefinedFunctionsProto() {
        return ProtoUtils.createUserDefinedDataStreamFunctionProtos(
                getPythonFunctionInfo(),
                getRuntimeContext(),
                new HashMap<>(),
                inBatchExecutionMode(getKeyedStateBackend()),
                config.get(PYTHON_METRIC_ENABLED),
                config.get(PYTHON_PROFILE_ENABLED),
                false,
                config.get(STATE_CACHE_SIZE),
                config.get(MAP_STATE_READ_CACHE_SIZE),
                config.get(MAP_STATE_WRITE_CACHE_SIZE));
    }

    @Override
    public Object getFunctionContext() {
        return null;
    }

    @Override
    public Object getTimerContext() {
        return null;
    }

    @Override
    public <T> AbstractEmbeddedDataStreamPythonFunctionOperator<T> copy(
            DataStreamPythonFunctionInfo pythonFunctionInfo, TypeInformation<T> outputTypeInfo) {
        return new EmbeddedPythonAsyncWaitOperator<>(
                config,
                pythonFunctionInfo,
                getInputTypeInfo(),
                outputTypeInfo,
                capacity,
                timeout,
                ordered);
    }

    /**
     * Completes the queue entry of one input. Python calls {@link #complete} or {@link
     * #completeExceptionally} from the event loop thread, or from the task thread when {@code
     * timeout} returns or raises. Only the first call takes effect.
     */
    public class Handler {

        private final StreamRecord<IN> inputRecord;

        private final ResultFuture<OUT> resultFuture;

        private final AtomicBoolean completed = new AtomicBoolean(false);

        private ScheduledFuture<?> timeoutTimer;

        Handler(StreamRecord<IN> inputRecord, ResultFuture<OUT> resultFuture) {
            this.inputRecord = inputRecord;
            this.resultFuture = resultFuture;
        }

        /** Completes the entry with the results in the form produced by the Python converters. */
        public void complete(Object results) {
            if (closing || !completed.compareAndSet(false, true)) {
                return;
            }
            try {
                final List<Object> externals = toList(results);
                mailboxExecutor.execute(
                        () -> processResults(externals),
                        "EmbeddedPythonAsyncWaitOperator#complete");
            } catch (Throwable t) {
                failAfterCompletion(t);
            }
        }

        /** Fails the task with the Python traceback and completes the entry without results. */
        public void completeExceptionally(String error) {
            if (closing || !completed.compareAndSet(false, true)) {
                return;
            }
            try {
                getContainingTask()
                        .getEnvironment()
                        .failExternally(
                                new Exception(
                                        "Could not complete the stream element: "
                                                + inputRecord
                                                + '.',
                                        new RuntimeException(error)));
                // complete with an empty result, so that the timer is removed and the queue
                // keeps draining
                mailboxExecutor.execute(
                        () -> processResults(Collections.emptyList()),
                        "EmbeddedPythonAsyncWaitOperator#completeExceptionally");
            } catch (Throwable t) {
                failAfterCompletion(t);
            }
        }

        /** The entry can't be completed any more, so the task must fail instead of waiting. */
        private void failAfterCompletion(Throwable t) {
            if (!closing) {
                getContainingTask()
                        .getEnvironment()
                        .failExternally(
                                new Exception(
                                        "Could not complete the stream element: "
                                                + inputRecord
                                                + '.',
                                        t));
            }
        }

        private void onTimeout() {
            // the result may already be on its way through the mailbox
            if (closing || completed.get()) {
                return;
            }
            invokeTimeout(this, inputConverter.toExternal(inputRecord.getValue()));
        }

        private void processResults(List<Object> externals) {
            if (closing) {
                return;
            }
            if (timeoutTimer != null) {
                timeoutTimer.cancel(true);
            }
            List<OUT> results = new ArrayList<>(externals.size());
            for (Object external : externals) {
                // the converters may reuse their output objects, so take a copy of each result
                results.add(outSerializer.copy(outputDataConverter.toInternal(external)));
            }
            resultFuture.complete(results);
            outputCompletedElement();
        }
    }

    private static List<Object> toList(Object results) {
        if (results instanceof Object[]) {
            return Arrays.asList((Object[]) results);
        }
        if (results instanceof Collection) {
            return new ArrayList<Object>((Collection<?>) results);
        }
        throw new IllegalArgumentException(
                "Unexpected result container from Python: "
                        + (results == null ? "null" : results.getClass().getName()));
    }
}
