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

package org.apache.flink.streaming.api.runners.python.beam;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.env.PythonEnvironment;
import org.apache.flink.python.env.process.ProcessPythonEnvironment;
import org.apache.flink.python.env.process.ProcessPythonEnvironmentManager;
import org.apache.flink.python.metric.process.FlinkMetricContainer;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerRegistration;
import org.apache.flink.streaming.api.operators.python.process.timer.TimerRegistrationAction;
import org.apache.flink.streaming.api.runners.python.beam.state.BeamStateRequestHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ShutdownHookUtil;
import org.apache.flink.util.TemporaryClassLoaderContext;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.LongFunctionWithException;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.TimerInternals;
import org.apache.beam.runners.fnexecution.control.BundleProgressHandler;
import org.apache.beam.runners.fnexecution.control.DefaultJobBundleFactory;
import org.apache.beam.runners.fnexecution.control.JobBundleFactory;
import org.apache.beam.runners.fnexecution.control.OutputReceiverFactory;
import org.apache.beam.runners.fnexecution.control.RemoteBundle;
import org.apache.beam.runners.fnexecution.control.StageBundleFactory;
import org.apache.beam.runners.fnexecution.control.TimerReceiverFactory;
import org.apache.beam.runners.fnexecution.provisioning.JobInfo;
import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.fn.data.FnDataReceiver;
import org.apache.beam.sdk.options.ExperimentalOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.options.SdkHarnessOptions;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.construction.Environments;
import org.apache.beam.sdk.util.construction.ModelCoders;
import org.apache.beam.sdk.util.construction.PipelineOptionsTranslation;
import org.apache.beam.sdk.util.construction.Timer;
import org.apache.beam.sdk.util.construction.graph.ExecutableStage;
import org.apache.beam.sdk.util.construction.graph.ImmutableExecutableStage;
import org.apache.beam.sdk.util.construction.graph.PipelineNode;
import org.apache.beam.sdk.util.construction.graph.SideInputReference;
import org.apache.beam.sdk.util.construction.graph.TimerReference;
import org.apache.beam.sdk.util.construction.graph.UserStateReference;
import org.apache.beam.sdk.values.WindowedValue;
import org.apache.beam.sdk.values.WindowedValues;
import org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.Struct;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import static org.apache.beam.sdk.util.construction.BeamUrns.getUrn;
import static org.apache.flink.python.Constants.INPUT_COLLECTION_ID;
import static org.apache.flink.python.Constants.OUTPUT_COLLECTION_ID;
import static org.apache.flink.python.Constants.SIDE_OUTPUT_CODER_PREFIX;
import static org.apache.flink.python.Constants.TIMER_CODER_ID;
import static org.apache.flink.python.Constants.WINDOW_CODER_ID;
import static org.apache.flink.python.Constants.WINDOW_STRATEGY;
import static org.apache.flink.python.Constants.WRAPPER_TIMER_CODER_ID;
import static org.apache.flink.python.PythonOptions.PYTHON_LOGGING_DEFAULT_LEVEL;
import static org.apache.flink.python.PythonOptions.PYTHON_LOGGING_LEVEL_OVERRIDE;
import static org.apache.flink.python.PythonOptions.USE_MANAGED_MEMORY;
import static org.apache.flink.python.util.ProtoUtils.createCoderProto;

/** A {@link BeamPythonFunctionRunner} used to execute Python functions. */
@Internal
public abstract class BeamPythonFunctionRunner implements PythonFunctionRunner {
    protected static final Logger LOG = LoggerFactory.getLogger(BeamPythonFunctionRunner.class);

    private static final String INPUT_CODER_ID = "input_coder";
    private static final String OUTPUT_CODER_ID = "output_coder";

    private static final String MANAGED_MEMORY_RESOURCE_ID = "python-process-managed-memory";
    private static final String PYTHON_WORKER_MEMORY_LIMIT = "_PYTHON_WORKER_MEMORY_LIMIT";

    private final String taskName;

    /** The Python process execution environment manager. */
    private final ProcessPythonEnvironmentManager environmentManager;

    /** The flinkMetricContainer will be set to null if metric is configured to be turned off. */
    @Nullable private final FlinkMetricContainer flinkMetricContainer;

    @Nullable private final KeyedStateBackend<?> keyedStateBackend;

    @Nullable private final OperatorStateBackend operatorStateBackend;

    @Nullable private final TypeSerializer<?> keySerializer;

    @Nullable private final TypeSerializer<?> namespaceSerializer;

    @Nullable private final TimerRegistration timerRegistration;

    private final MemoryManager memoryManager;

    /** The fraction of total managed memory in the slot that the Python worker could use. */
    private final double managedMemoryFraction;

    protected final FlinkFnApi.CoderInfoDescriptor inputCoderDescriptor;
    protected final FlinkFnApi.CoderInfoDescriptor outputCoderDescriptor;
    protected final Map<String, FlinkFnApi.CoderInfoDescriptor> sideOutputCoderDescriptors;

    // ------------------------------------------------------------------------

    private transient boolean bundleStarted;

    /**
     * The bundle factory which has all job-scoped information and can be used to create a {@link
     * StageBundleFactory}.
     */
    private transient JobBundleFactory jobBundleFactory;

    /**
     * The bundle factory which has all of the resources it needs to provide new {@link
     * RemoteBundle}.
     */
    private transient StageBundleFactory stageBundleFactory;

    /** Handler for state requests. */
    private transient BeamStateRequestHandler stateRequestHandler;

    /** Handler for bundle progress messages, both during bundle execution and on its completion. */
    private transient BundleProgressHandler progressHandler;

    /**
     * A bundle handler for handling input elements by forwarding them to a remote environment for
     * processing. It holds a collection of {@link FnDataReceiver}s which actually perform the data
     * forwarding work.
     *
     * <p>When a RemoteBundle is closed, it will block until bundle processing is finished on remote
     * resources, and throw an exception if bundle processing has failed.
     */
    private transient RemoteBundle remoteBundle;

    /** The Python function execution result tuple: (output tag, raw bytes, length). */
    private transient Tuple3<String, byte[], Integer> reusableResultTuple;

    /** Buffers the Python function execution result which has still not been processed. */
    @VisibleForTesting protected transient LinkedBlockingQueue<Tuple2<String, byte[]>> resultBuffer;

    /** The receiver which forwards the input elements to a remote environment for processing. */
    @VisibleForTesting protected transient FnDataReceiver<WindowedValue<byte[]>> mainInputReceiver;

    /** The receiver which forwards the timer data to a remote environment for processing. */
    private transient FnDataReceiver<Timer> timerInputReceiver;

    /** The shared resource among Python operators of the same slot. */
    private transient OpaqueMemoryResource<PythonSharedResources> sharedResources;

    /** Prevents duplicate teardown from explicit close and the JVM shutdown hook. */
    private transient boolean closed;

    /** Coordinates duplicate close calls until teardown has finished. */
    @Nullable private transient CompletableFuture<Void> closeCompletion;

    /** Whether cancellation should skip waiting for an in-flight bundle close. */
    private transient boolean cancelRequested;

    /** Prevents duplicate resource teardown from close and cancellation. */
    private transient boolean resourcesClosed;

    /** Coordinates duplicate close and cancel calls while the state handler drains requests. */
    @Nullable private transient CompletableFuture<Void> stateHandlerCloseCompletion;

    /** Owns a detached bundle close after cancellation returns to the task cleanup path. */
    @Nullable private transient ExecutorService cancellationBundleCloseExecutor;

    /**
     * The bundle close currently owned by a flush thread.
     *
     * <p>The operation remains reachable while it is running so a concurrent graceful close can
     * wait for the same bundle close without attempting a second one.
     */
    @Nullable private transient BundleCloseOperation bundleCloseOperation;

    private transient Thread shutdownHook;

    private transient Environment environment;

    private transient volatile List<TimerRegistrationAction> unregisteredTimers;

    public BeamPythonFunctionRunner(
            Environment environment,
            String taskName,
            ProcessPythonEnvironmentManager environmentManager,
            @Nullable FlinkMetricContainer flinkMetricContainer,
            @Nullable KeyedStateBackend<?> keyedStateBackend,
            @Nullable OperatorStateBackend operatorStateBackend,
            @Nullable TypeSerializer<?> keySerializer,
            @Nullable TypeSerializer<?> namespaceSerializer,
            @Nullable TimerRegistration timerRegistration,
            MemoryManager memoryManager,
            double managedMemoryFraction,
            FlinkFnApi.CoderInfoDescriptor inputCoderDescriptor,
            FlinkFnApi.CoderInfoDescriptor outputCoderDescriptor,
            Map<String, FlinkFnApi.CoderInfoDescriptor> sideOutputCoderDescriptors) {
        this.environment = environment;
        this.taskName = Preconditions.checkNotNull(taskName);
        this.environmentManager = Preconditions.checkNotNull(environmentManager);
        this.flinkMetricContainer = flinkMetricContainer;
        this.keyedStateBackend = keyedStateBackend;
        this.operatorStateBackend = operatorStateBackend;
        this.keySerializer = keySerializer;
        this.namespaceSerializer = namespaceSerializer;
        this.timerRegistration = timerRegistration;
        this.memoryManager = memoryManager;
        this.managedMemoryFraction = managedMemoryFraction;
        this.inputCoderDescriptor = Preconditions.checkNotNull(inputCoderDescriptor);
        this.outputCoderDescriptor = Preconditions.checkNotNull(outputCoderDescriptor);
        this.sideOutputCoderDescriptors = Preconditions.checkNotNull(sideOutputCoderDescriptors);
    }

    // ------------------------------------------------------------------------

    @Override
    public void open(ReadableConfig config) throws Exception {
        this.bundleStarted = false;
        this.resultBuffer = new LinkedBlockingQueue<>();
        this.reusableResultTuple = new Tuple3<>();

        stateRequestHandler =
                getStateRequestHandler(
                        keyedStateBackend,
                        operatorStateBackend,
                        keySerializer,
                        namespaceSerializer,
                        config);

        // The creation of stageBundleFactory depends on the initialized environment manager.
        environmentManager.open();

        PortablePipelineOptions portableOptions;
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(getClass().getClassLoader())) {
            // It loads classes using service loader under context classloader in Beam,
            // make sure the classloader used to load SPI classes is the same as the class loader of
            // the current class.
            portableOptions = PipelineOptionsFactory.as(PortablePipelineOptions.class);
        }

        int stateCacheSize = config.get(PythonOptions.STATE_CACHE_SIZE);
        if (stateCacheSize > 0) {
            portableOptions
                    .as(ExperimentalOptions.class)
                    .setExperiments(
                            Collections.singletonList(
                                    ExperimentalOptions.STATE_CACHE_SIZE + "=" + stateCacheSize));
        }

        // Set log level
        portableOptions
                .as(SdkHarnessOptions.class)
                .setDefaultSdkHarnessLogLevel(
                        SdkHarnessOptions.LogLevel.valueOf(
                                config.get(PYTHON_LOGGING_DEFAULT_LEVEL)));
        if (config.getOptional(PYTHON_LOGGING_LEVEL_OVERRIDE).isPresent()) {
            SdkHarnessOptions.SdkHarnessLogLevelOverrides loggingOptions =
                    SdkHarnessOptions.SdkHarnessLogLevelOverrides.from(
                            config.get(PYTHON_LOGGING_LEVEL_OVERRIDE));
            portableOptions
                    .as(SdkHarnessOptions.class)
                    .setSdkHarnessLogLevelOverrides(loggingOptions);
        }

        Struct pipelineOptions = PipelineOptionsTranslation.toProto(portableOptions);

        if (memoryManager != null
                && config.get(USE_MANAGED_MEMORY)
                && managedMemoryFraction > 0
                && managedMemoryFraction <= 1.0) {
            final LongFunctionWithException<PythonSharedResources, Exception> initializer =
                    (size) ->
                            new PythonSharedResources(
                                    createJobBundleFactory(pipelineOptions),
                                    createPythonExecutionEnvironment(config, size));

            sharedResources =
                    memoryManager.getSharedMemoryResourceForManagedMemory(
                            MANAGED_MEMORY_RESOURCE_ID, initializer, managedMemoryFraction);
            LOG.info("Obtained shared Python process of size {} bytes", sharedResources.getSize());
            sharedResources.getResourceHandle().addPythonEnvironmentManager(environmentManager);

            JobBundleFactory jobBundleFactory =
                    sharedResources.getResourceHandle().getJobBundleFactory();
            RunnerApi.Environment environment =
                    sharedResources.getResourceHandle().getEnvironment();
            stageBundleFactory = createStageBundleFactory(jobBundleFactory, environment);
        } else {
            if (memoryManager != null
                    && config.get(USE_MANAGED_MEMORY)
                    && (managedMemoryFraction <= 0 || managedMemoryFraction > 1.0)) {
                LOG.warn(
                        String.format(
                                "The configured managed memory fraction for Python worker process must be within (0, 1], was: %s, use off-heap memory instead."
                                        + "Please see config option \"taskmanager.memory.managed.consumer-weights\" for more details.",
                                managedMemoryFraction));
            }
            // there is no way to access the MemoryManager for the batch job of old planner,
            // fallback to the way that spawning a Python process for each Python operator
            jobBundleFactory = createJobBundleFactory(pipelineOptions);
            stageBundleFactory =
                    createStageBundleFactory(
                            jobBundleFactory, createPythonExecutionEnvironment(config, -1));
        }
        progressHandler = getProgressHandler(flinkMetricContainer);

        shutdownHook =
                ShutdownHookUtil.addShutdownHook(
                        this::cancel, BeamPythonFunctionRunner.class.getSimpleName(), LOG);

        unregisteredTimers = Collections.synchronizedList(new LinkedList<>());
    }

    @Override
    public void close() throws Exception {
        if (!startClose()) {
            awaitCloseCompletion();
            return;
        }

        try {
            try {
                // Normal shutdown finishes this runner's bundle before tearing down its factory or
                // shared lease. Cancellation uses the separate non-blocking path below.
                flush();
            } finally {
                try {
                    closeResources();
                } finally {
                    try {
                        closeStateRequestHandler();
                    } finally {
                        removeShutdownHook();
                    }
                }
            }
        } finally {
            completeClose();
        }
    }

    @Override
    public void cancel() throws Exception {
        final BundleCloseClaim bundleCloseClaim = requestCancel();

        try {
            if (bundleCloseClaim != null && bundleCloseClaim.ownsBundleClose) {
                closeBundleAfterCancellation(bundleCloseClaim.closeOperation);
            }
            // Release only this runner's resource lease. If it is the final lease, the shared
            // resource disposer owns factory teardown; otherwise co-located runners keep using the
            // shared worker.
            closeResources();
        } finally {
            try {
                // A bundle close already owned by another thread may still issue callbacks while
                // it unwinds. Gate those callbacks before state backends are disposed.
                closeStateRequestHandler();
            } finally {
                try {
                    removeShutdownHook();
                } finally {
                    completeClose();
                }
            }
        }
    }

    @Override
    public void process(byte[] data) throws Exception {
        final FnDataReceiver<WindowedValue<byte[]>> inputReceiver;
        synchronized (this) {
            checkInvokeStartBundle();
            inputReceiver = mainInputReceiver;
        }
        inputReceiver.accept(WindowedValues.valueInGlobalWindow(data));
    }

    @Override
    public void drainUnregisteredTimers() {
        synchronized (unregisteredTimers) {
            for (TimerRegistrationAction timerRegistrationAction : unregisteredTimers) {
                timerRegistrationAction.registerTimer();
            }
            unregisteredTimers.clear();
        }
    }

    @Override
    public void processTimer(byte[] timerData) throws Exception {
        final FnDataReceiver<Timer> inputReceiver;
        synchronized (this) {
            if (timerInputReceiver == null) {
                checkInvokeStartBundle();
                timerInputReceiver =
                        Preconditions.checkNotNull(
                                Iterables.getOnlyElement(remoteBundle.getTimerReceivers().values()),
                                "Failed to retrieve main input receiver.");
            }
            inputReceiver = timerInputReceiver;
        }

        Timer<byte[]> timerValue = Timer.cleared(timerData, "", Collections.emptyList());
        inputReceiver.accept(timerValue);
    }

    /** Checks whether to invoke startBundle. */
    private void checkInvokeStartBundle() {
        if (closed) {
            throw new IllegalStateException("Beam Python function runner is closed.");
        }
        if (!bundleStarted) {
            if (bundleCloseOperation != null && !bundleCloseOperation.isDone()) {
                throw new IllegalStateException("The previous Beam bundle is still closing.");
            }
            startBundle();
            bundleStarted = true;
            bundleCloseOperation = null;
        }
    }

    @VisibleForTesting
    protected void startBundle() {
        try {
            remoteBundle =
                    stageBundleFactory.getBundle(
                            createOutputReceiverFactory(),
                            createTimerReceiverFactory(),
                            stateRequestHandler,
                            progressHandler);
            mainInputReceiver =
                    Preconditions.checkNotNull(
                            Iterables.getOnlyElement(remoteBundle.getInputReceivers().values()),
                            "Failed to retrieve main input receiver.");
        } catch (Throwable t) {
            throw new RuntimeException("Failed to start remote bundle", t);
        }
    }

    @Override
    public Tuple3<String, byte[], Integer> pollResult() throws Exception {
        Tuple2<String, byte[]> result = resultBuffer.poll();
        if (result == null) {
            return null;
        }
        reusableResultTuple.f0 = result.f0;
        reusableResultTuple.f1 = result.f1;
        reusableResultTuple.f2 = result.f1.length;
        return reusableResultTuple;
    }

    @Override
    public Tuple3<String, byte[], Integer> takeResult() throws Exception {
        Tuple2<String, byte[]> result = resultBuffer.take();
        reusableResultTuple.f0 = result.f0;
        reusableResultTuple.f1 = result.f1;
        reusableResultTuple.f2 = result.f1.length;
        return reusableResultTuple;
    }

    @Override
    public void flush() throws Exception {
        final BundleCloseOperation closeOperation;
        final boolean ownsBundleClose;
        synchronized (this) {
            if (cancelRequested) {
                return;
            }

            if (bundleStarted) {
                closeOperation = new BundleCloseOperation(remoteBundle);
                bundleCloseOperation = closeOperation;
                bundleStarted = false;
                clearBundleReferences();
                ownsBundleClose = true;
            } else {
                closeOperation = bundleCloseOperation;
                ownsBundleClose = false;
            }
        }

        if (closeOperation == null) {
            return;
        }

        if (ownsBundleClose) {
            finishBundle(closeOperation);
        } else {
            closeOperation.await();
        }
    }

    /** Interrupts the progress of takeResult. */
    public void notifyNoMoreResults() {
        resultBuffer.add(Tuple2.of(null, new byte[0]));
    }

    private void finishBundle(BundleCloseOperation closeOperation) {
        RuntimeException failure = null;
        try {
            closeOperation.remoteBundle.close();
        } catch (Throwable t) {
            failure = new RuntimeException("Failed to close remote bundle", t);
            throw failure;
        } finally {
            closeOperation.complete(failure);
            if (isCancelRequested()) {
                try {
                    closeResources();
                } catch (Exception e) {
                    LOG.warn("Failed to clean up Python resources after cancellation.", e);
                }
            }
        }
    }

    private synchronized boolean startClose() {
        if (closed) {
            return false;
        }
        closed = true;
        closeCompletion = new CompletableFuture<>();
        return true;
    }

    /**
     * Starts cancellation without waiting for a concurrent bundle close.
     *
     * <p>If no flush thread owns the active bundle yet, cancellation detaches it and becomes
     * responsible for finishing it asynchronously.
     */
    @Nullable
    private synchronized BundleCloseClaim requestCancel() {
        cancelRequested = true;
        closed = true;
        if (closeCompletion == null) {
            closeCompletion = new CompletableFuture<>();
        }

        if (bundleStarted) {
            final BundleCloseOperation closeOperation = new BundleCloseOperation(remoteBundle);
            bundleCloseOperation = closeOperation;
            bundleStarted = false;
            clearBundleReferences();
            return new BundleCloseClaim(closeOperation, true);
        }
        if (bundleCloseOperation != null && !bundleCloseOperation.isDone()) {
            return new BundleCloseClaim(bundleCloseOperation, false);
        }
        clearBundleReferences();
        return null;
    }

    private synchronized boolean isCancelRequested() {
        return cancelRequested;
    }

    private void awaitCloseCompletion() throws Exception {
        final CompletableFuture<Void> currentCloseCompletion;
        synchronized (this) {
            currentCloseCompletion = closeCompletion;
        }
        if (currentCloseCompletion != null) {
            awaitCompletion(currentCloseCompletion);
        }
    }

    private void completeClose() {
        final CompletableFuture<Void> currentCloseCompletion;
        synchronized (this) {
            currentCloseCompletion = closeCompletion;
        }
        if (currentCloseCompletion != null) {
            currentCloseCompletion.complete(null);
        }
    }

    private void closeResources() throws Exception {
        final JobBundleFactory ownedJobBundleFactory;
        final OpaqueMemoryResource<PythonSharedResources> currentSharedResources;
        synchronized (this) {
            if (resourcesClosed) {
                return;
            }
            resourcesClosed = true;
            ownedJobBundleFactory = jobBundleFactory;
            currentSharedResources = sharedResources;
            jobBundleFactory = null;
            sharedResources = null;
        }

        if (currentSharedResources != null) {
            try {
                currentSharedResources.close();
            } finally {
                clearBundleReferences();
            }
        } else {
            try {
                if (ownedJobBundleFactory != null) {
                    ownedJobBundleFactory.close();
                }
            } finally {
                try {
                    environmentManager.close();
                } finally {
                    clearBundleReferences();
                }
            }
        }
    }

    private void closeBundleAfterCancellation(BundleCloseOperation closeOperation) {
        final ExecutorService executor;
        synchronized (this) {
            if (cancellationBundleCloseExecutor == null) {
                cancellationBundleCloseExecutor =
                        Executors.newSingleThreadExecutor(
                                new ExecutorThreadFactory("beam-python-bundle-cancellation"));
            }
            executor = cancellationBundleCloseExecutor;
        }
        executor.execute(
                () -> {
                    try {
                        finishBundle(closeOperation);
                    } catch (Throwable t) {
                        LOG.debug("Beam bundle close failed during cancellation.", t);
                    } finally {
                        executor.shutdown();
                    }
                });
    }

    private void closeStateRequestHandler() throws Exception {
        final BeamStateRequestHandler currentStateRequestHandler;
        final CompletableFuture<Void> closeCompletion;
        final boolean ownsClose;
        synchronized (this) {
            if (stateHandlerCloseCompletion == null) {
                closeCompletion = new CompletableFuture<>();
                stateHandlerCloseCompletion = closeCompletion;
                currentStateRequestHandler = stateRequestHandler;
                stateRequestHandler = null;
                ownsClose = true;
            } else {
                closeCompletion = stateHandlerCloseCompletion;
                currentStateRequestHandler = null;
                ownsClose = false;
            }
        }

        if (ownsClose) {
            RuntimeException failure = null;
            try {
                if (currentStateRequestHandler != null) {
                    currentStateRequestHandler.close();
                }
            } catch (RuntimeException e) {
                failure = e;
                throw e;
            } finally {
                if (failure == null) {
                    closeCompletion.complete(null);
                } else {
                    closeCompletion.completeExceptionally(failure);
                }
            }
        } else {
            awaitCompletion(closeCompletion);
        }
    }

    private synchronized void clearBundleReferences() {
        remoteBundle = null;
        mainInputReceiver = null;
        timerInputReceiver = null;
    }

    private void removeShutdownHook() {
        final Thread currentShutdownHook;
        synchronized (this) {
            currentShutdownHook = shutdownHook;
            shutdownHook = null;
        }
        if (currentShutdownHook != null) {
            ShutdownHookUtil.removeShutdownHook(
                    currentShutdownHook, BeamPythonFunctionRunner.class.getSimpleName(), LOG);
        }
    }

    private static final class BundleCloseOperation {

        private final RemoteBundle remoteBundle;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();

        private BundleCloseOperation(RemoteBundle remoteBundle) {
            this.remoteBundle = remoteBundle;
        }

        private boolean isDone() {
            return completion.isDone();
        }

        private void complete(@Nullable RuntimeException failure) {
            if (failure == null) {
                completion.complete(null);
            } else {
                completion.completeExceptionally(failure);
            }
        }

        private void await() throws Exception {
            awaitCompletion(completion);
        }
    }

    private static final class BundleCloseClaim {

        private final BundleCloseOperation closeOperation;
        private final boolean ownsBundleClose;

        private BundleCloseClaim(BundleCloseOperation closeOperation, boolean ownsBundleClose) {
            this.closeOperation = closeOperation;
            this.ownsBundleClose = ownsBundleClose;
        }
    }

    private static void awaitCompletion(CompletableFuture<Void> completion) throws Exception {
        try {
            completion.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        } catch (ExecutionException e) {
            final Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    // ------------------------------------------------------------------------
    // Python Execution Environment Management
    // ------------------------------------------------------------------------

    /**
     * Creates a specification which specifies the portability Python execution environment. It's
     * used by Beam's portability framework to creates the actual Python execution environment.
     */
    private RunnerApi.Environment createPythonExecutionEnvironment(
            ReadableConfig config, long memoryLimitBytes) throws Exception {
        PythonEnvironment environment = environmentManager.createEnvironment();
        if (environment instanceof ProcessPythonEnvironment) {
            ProcessPythonEnvironment processEnvironment = (ProcessPythonEnvironment) environment;
            Map<String, String> env = processEnvironment.getEnv();
            config.getOptional(PythonOptions.PYTHON_JOB_OPTIONS).ifPresent(env::putAll);
            env.put(PYTHON_WORKER_MEMORY_LIMIT, String.valueOf(memoryLimitBytes));
            return Environments.createProcessEnvironment(
                    "", "", processEnvironment.getCommand(), env);
        }
        throw new RuntimeException("Currently only ProcessPythonEnvironment is supported.");
    }

    // ------------------------------------------------------------------------
    // Construct ExecutableStage
    // ------------------------------------------------------------------------

    /**
     * Creates a {@link ExecutableStage} which contains the Python user-defined functions to be
     * executed and all the other information needed to execute them, such as the execution
     * environment, the input and output coder, etc.
     */
    @SuppressWarnings("unchecked")
    private ExecutableStage createExecutableStage(RunnerApi.Environment environment)
            throws Exception {
        RunnerApi.Components.Builder componentsBuilder =
                RunnerApi.Components.newBuilder()
                        .putPcollections(
                                INPUT_COLLECTION_ID,
                                RunnerApi.PCollection.newBuilder()
                                        .setWindowingStrategyId(WINDOW_STRATEGY)
                                        .setCoderId(INPUT_CODER_ID)
                                        .build())
                        .putPcollections(
                                OUTPUT_COLLECTION_ID,
                                RunnerApi.PCollection.newBuilder()
                                        .setWindowingStrategyId(WINDOW_STRATEGY)
                                        .setCoderId(OUTPUT_CODER_ID)
                                        .build())
                        .putWindowingStrategies(
                                WINDOW_STRATEGY,
                                RunnerApi.WindowingStrategy.newBuilder()
                                        .setWindowCoderId(WINDOW_CODER_ID)
                                        .build())
                        .putCoders(INPUT_CODER_ID, createCoderProto(inputCoderDescriptor))
                        .putCoders(OUTPUT_CODER_ID, createCoderProto(outputCoderDescriptor))
                        .putCoders(WINDOW_CODER_ID, getWindowCoderProto());

        for (Map.Entry<String, FlinkFnApi.CoderInfoDescriptor> entry :
                sideOutputCoderDescriptors.entrySet()) {
            String collectionId = entry.getKey();
            String collectionCoderId = SIDE_OUTPUT_CODER_PREFIX + collectionId;
            componentsBuilder.putPcollections(
                    collectionId,
                    RunnerApi.PCollection.newBuilder()
                            .setWindowingStrategyId(WINDOW_STRATEGY)
                            .setCoderId(collectionCoderId)
                            .build());
            componentsBuilder.putCoders(collectionCoderId, createCoderProto(entry.getValue()));
        }

        getOptionalTimerCoderProto()
                .ifPresent(
                        timerCoderProto -> {
                            componentsBuilder.putCoders(TIMER_CODER_ID, timerCoderProto);
                            RunnerApi.Coder wrapperTimerCoderProto =
                                    RunnerApi.Coder.newBuilder()
                                            .setSpec(
                                                    RunnerApi.FunctionSpec.newBuilder()
                                                            .setUrn(ModelCoders.TIMER_CODER_URN)
                                                            .build())
                                            .addComponentCoderIds(TIMER_CODER_ID)
                                            .addComponentCoderIds(WINDOW_CODER_ID)
                                            .build();
                            componentsBuilder.putCoders(
                                    WRAPPER_TIMER_CODER_ID, wrapperTimerCoderProto);
                        });

        buildTransforms(componentsBuilder);
        RunnerApi.Components components = componentsBuilder.build();

        PipelineNode.PCollectionNode input =
                PipelineNode.pCollection(
                        INPUT_COLLECTION_ID,
                        components.getPcollectionsOrThrow(INPUT_COLLECTION_ID));
        List<SideInputReference> sideInputs = Collections.EMPTY_LIST;
        List<UserStateReference> userStates = Collections.EMPTY_LIST;
        List<TimerReference> timers = getTimers(components);
        List<PipelineNode.PTransformNode> transforms =
                components.getTransformsMap().keySet().stream()
                        .map(id -> PipelineNode.pTransform(id, components.getTransformsOrThrow(id)))
                        .collect(Collectors.toList());
        List<PipelineNode.PCollectionNode> outputs = new ArrayList<>();
        outputs.add(
                PipelineNode.pCollection(
                        OUTPUT_COLLECTION_ID,
                        components.getPcollectionsOrThrow(OUTPUT_COLLECTION_ID)));
        for (Map.Entry<String, FlinkFnApi.CoderInfoDescriptor> entry :
                sideOutputCoderDescriptors.entrySet()) {
            String collectionId = entry.getKey();
            outputs.add(
                    PipelineNode.pCollection(
                            collectionId, components.getPcollectionsOrThrow(collectionId)));
        }
        return ImmutableExecutableStage.of(
                components,
                environment,
                input,
                sideInputs,
                userStates,
                timers,
                transforms,
                outputs,
                createValueOnlyWireCoderSetting());
    }

    private Collection<RunnerApi.ExecutableStagePayload.WireCoderSetting>
            createValueOnlyWireCoderSetting() throws IOException {
        WindowedValue<byte[]> value = WindowedValues.valueInGlobalWindow(new byte[0]);
        Coder<? extends BoundedWindow> windowCoder = GlobalWindow.Coder.INSTANCE;
        WindowedValues.FullWindowedValueCoder<byte[]> windowedValueCoder =
                WindowedValues.FullWindowedValueCoder.of(ByteArrayCoder.of(), windowCoder);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        windowedValueCoder.encode(value, baos);

        List<RunnerApi.ExecutableStagePayload.WireCoderSetting> settings = new ArrayList<>();
        settings.add(
                RunnerApi.ExecutableStagePayload.WireCoderSetting.newBuilder()
                        .setUrn(getUrn(RunnerApi.StandardCoders.Enum.PARAM_WINDOWED_VALUE))
                        .setPayload(
                                org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString
                                        .copyFrom(baos.toByteArray()))
                        .setInputOrOutputId(INPUT_COLLECTION_ID)
                        .build());
        settings.add(
                RunnerApi.ExecutableStagePayload.WireCoderSetting.newBuilder()
                        .setUrn(getUrn(RunnerApi.StandardCoders.Enum.PARAM_WINDOWED_VALUE))
                        .setPayload(
                                org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf.ByteString
                                        .copyFrom(baos.toByteArray()))
                        .setInputOrOutputId(OUTPUT_COLLECTION_ID)
                        .build());
        for (Map.Entry<String, FlinkFnApi.CoderInfoDescriptor> entry :
                sideOutputCoderDescriptors.entrySet()) {
            settings.add(
                    RunnerApi.ExecutableStagePayload.WireCoderSetting.newBuilder()
                            .setUrn(getUrn(RunnerApi.StandardCoders.Enum.PARAM_WINDOWED_VALUE))
                            .setPayload(
                                    org.apache.beam.vendor.grpc.v1p69p0.com.google.protobuf
                                            .ByteString.copyFrom(baos.toByteArray()))
                            .setInputOrOutputId(entry.getKey())
                            .build());
        }
        return settings;
    }

    /** Gets the proto representation of the window coder. */
    private RunnerApi.Coder getWindowCoderProto() {
        return RunnerApi.Coder.newBuilder()
                .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                                .setUrn(ModelCoders.GLOBAL_WINDOW_CODER_URN)
                                .build())
                .build();
    }

    protected abstract void buildTransforms(RunnerApi.Components.Builder componentsBuilder);

    protected abstract List<TimerReference> getTimers(RunnerApi.Components components);

    protected abstract Optional<RunnerApi.Coder> getOptionalTimerCoderProto();

    // ------------------------------------------------------------------------
    // Construct RemoteBundler
    // ------------------------------------------------------------------------

    private OutputReceiverFactory createOutputReceiverFactory() {
        return new OutputReceiverFactory() {

            // the input value type is always byte array
            @SuppressWarnings("unchecked")
            @Override
            public FnDataReceiver<WindowedValue<byte[]>> create(String pCollectionId) {
                return input -> {
                    resultBuffer.add(Tuple2.of(pCollectionId, input.getValue()));
                };
            }
        };
    }

    @VisibleForTesting
    public JobBundleFactory createJobBundleFactory(Struct pipelineOptions) throws Exception {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(getClass().getClassLoader())) {
            // It loads classes using service loader under context classloader in Beam,
            // make sure the classloader used to load SPI classes is the same as the class
            // loader of the current class.
            return DefaultJobBundleFactory.create(
                    JobInfo.create(
                            taskName,
                            taskName,
                            environmentManager.createRetrievalToken(),
                            pipelineOptions));
        }
    }

    /** To make the error messages more user friendly, throws an exception with the boot logs. */
    private StageBundleFactory createStageBundleFactory(
            JobBundleFactory jobBundleFactory, RunnerApi.Environment environment) throws Exception {
        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(getClass().getClassLoader())) {
            // It loads classes using service loader under context classloader in Beam,
            // make sure the classloader used to load SPI classes is the same as the class
            // loader of the current class.
            return jobBundleFactory.forStage(createExecutableStage(environment));
        } catch (Throwable e) {
            throw new RuntimeException(environmentManager.getBootLog(), e);
        }
    }

    /**
     * Ignore bundle progress if flinkMetricContainer is null. The flinkMetricContainer will be set
     * to null if metric is configured to be turned off.
     */
    private BundleProgressHandler getProgressHandler(FlinkMetricContainer flinkMetricContainer) {
        if (flinkMetricContainer == null) {
            return BundleProgressHandler.ignored();
        } else {
            return new BundleProgressHandler() {
                @Override
                public void onProgress(BeamFnApi.ProcessBundleProgressResponse progress) {
                    flinkMetricContainer.updateMetrics(taskName, progress.getMonitoringInfosList());
                }

                @Override
                public void onCompleted(BeamFnApi.ProcessBundleResponse response) {
                    flinkMetricContainer.updateMetrics(taskName, response.getMonitoringInfosList());
                }
            };
        }
    }

    private TimerReceiverFactory createTimerReceiverFactory() {
        BiConsumer<Timer<?>, TimerInternals.TimerData> timerDataConsumer =
                (timer, timerData) -> {
                    TimerRegistrationAction timerRegistrationAction =
                            new TimerRegistrationAction(
                                    timerRegistration,
                                    (byte[]) timer.getUserKey(),
                                    unregisteredTimers);
                    unregisteredTimers.add(timerRegistrationAction);
                    environment
                            .getMainMailboxExecutor()
                            .execute(timerRegistrationAction::run, "PythonTimerRegistration");
                };
        return new TimerReceiverFactory(stageBundleFactory, timerDataConsumer, null);
    }

    private static BeamStateRequestHandler getStateRequestHandler(
            @Nullable KeyedStateBackend<?> keyedStateBackend,
            @Nullable OperatorStateBackend operatorStateBackend,
            @Nullable TypeSerializer<?> keySerializer,
            @Nullable TypeSerializer<?> namespaceSerializer,
            ReadableConfig config) {
        return BeamStateRequestHandler.of(
                keyedStateBackend,
                operatorStateBackend,
                keySerializer,
                namespaceSerializer,
                config);
    }
}
