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

package org.apache.flink.streaming.api.operators.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonConfig;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.python.env.PythonDependencyInfo;
import org.apache.flink.python.env.PythonEnvironmentManager;
import org.apache.flink.python.env.beam.ProcessPythonEnvironmentManager;
import org.apache.flink.python.metric.FlinkMetricContainer;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.InternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionInternalTimeServiceManager;
import org.apache.flink.streaming.api.operators.sorted.state.BatchExecutionKeyedStateBackend;
import org.apache.flink.streaming.api.runners.python.beam.BeamPythonFunctionRunner;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.functions.python.PythonEnv;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.WrappingRuntimeException;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.streaming.api.utils.ClassLeakCleaner.cleanUpLeakingClasses;
import static org.apache.flink.streaming.api.utils.PythonOperatorUtils.inBatchExecutionMode;

/** Base class for all stream operators to execute Python functions. */
@Internal
public abstract class AbstractPythonFunctionOperator<OUT> extends AbstractStreamOperator<OUT> {

    private static final long serialVersionUID = 1L;

    protected Configuration config;

    /**
     * The {@link PythonFunctionRunner} which is responsible for Python user-defined function
     * execution.
     */
    protected transient PythonFunctionRunner pythonFunctionRunner;

    /** Max number of elements to include in a bundle. */
    protected transient int maxBundleSize;

    /** Number of processed elements in the current bundle. */
    protected transient int elementCount;

    /** The python config. */
    protected transient PythonConfig pythonConfig;

    /** The options used to configure the Python worker process. */
    protected transient Map<String, String> jobOptions;

    /** Max duration of a bundle. */
    private transient long maxBundleTimeMills;

    /** Time that the last bundle was finished. */
    private transient long lastFinishBundleTime;

    /** A timer that finishes the current bundle after a fixed amount of time. */
    private transient ScheduledFuture<?> checkFinishBundleTimer;

    /** Callback to be executed after the current bundle was finished. */
    private transient Runnable bundleFinishedCallback;

    private transient ExecutorService flushThreadPool;

    public AbstractPythonFunctionOperator(Configuration config) {
        this.config = Preconditions.checkNotNull(config);
        this.chainingStrategy = ChainingStrategy.ALWAYS;
    }

    @Override
    public void open() throws Exception {
        try {
            this.pythonConfig = new PythonConfig(config);
            this.jobOptions = config.toMap();
            this.maxBundleSize = pythonConfig.getMaxBundleSize();
            if (this.maxBundleSize <= 0) {
                this.maxBundleSize = PythonOptions.MAX_BUNDLE_SIZE.defaultValue();
                LOG.error(
                        "Invalid value for the maximum bundle size. Using default value of "
                                + this.maxBundleSize
                                + '.');
            } else {
                LOG.info("The maximum bundle size is configured to {}.", this.maxBundleSize);
            }

            this.maxBundleTimeMills = pythonConfig.getMaxBundleTimeMills();
            if (this.maxBundleTimeMills <= 0L) {
                this.maxBundleTimeMills = PythonOptions.MAX_BUNDLE_TIME_MILLS.defaultValue();
                LOG.error(
                        "Invalid value for the maximum bundle time. Using default value of "
                                + this.maxBundleTimeMills
                                + '.');
            } else {
                LOG.info(
                        "The maximum bundle time is configured to {} milliseconds.",
                        this.maxBundleTimeMills);
            }

            this.pythonFunctionRunner = createPythonFunctionRunner();
            this.pythonFunctionRunner.open(pythonConfig);

            this.elementCount = 0;
            this.lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();

            // Schedule timer to check timeout of finish bundle.
            long bundleCheckPeriod = Math.max(this.maxBundleTimeMills, 1);
            this.checkFinishBundleTimer =
                    getProcessingTimeService()
                            .scheduleAtFixedRate(
                                    // ProcessingTimeService callbacks are executed under the
                                    // checkpointing lock
                                    timestamp -> checkInvokeFinishBundleByTime(),
                                    bundleCheckPeriod,
                                    bundleCheckPeriod);
            this.flushThreadPool = Executors.newSingleThreadExecutor();
        } finally {
            super.open();
        }
    }

    @Override
    public void finish() throws Exception {
        try {
            invokeFinishBundle();
        } finally {
            super.finish();
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (checkFinishBundleTimer != null) {
                checkFinishBundleTimer.cancel(true);
                checkFinishBundleTimer = null;
            }
            if (pythonFunctionRunner != null) {
                pythonFunctionRunner.close();
                pythonFunctionRunner = null;
            }
            if (flushThreadPool != null) {
                flushThreadPool.shutdown();
                flushThreadPool = null;
            }
        } finally {
            super.close();

            try {
                cleanUpLeakingClasses(this.getClass().getClassLoader());
            } catch (Throwable t) {
                LOG.warn("Failed to clean up the leaking objects.", t);
            }
        }
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        try {
            invokeFinishBundle();
        } finally {
            super.prepareSnapshotPreBarrier(checkpointId);
        }
    }

    @Override
    public void processWatermark(Watermark mark) throws Exception {
        // Due to the asynchronous communication with the SDK harness,
        // a bundle might still be in progress and not all items have
        // yet been received from the SDK harness. If we just set this
        // watermark as the new output watermark, we could violate the
        // order of the records, i.e. pending items in the SDK harness
        // could become "late" although they were "on time".
        //
        // We can solve this problem using one of the following options:
        //
        // 1) Finish the current bundle and emit this watermark as the
        //    new output watermark. Finishing the bundle ensures that
        //    all the items have been processed by the SDK harness and
        //    the execution results sent to the downstream operator.
        //
        // 2) Hold on the output watermark for as long as the current
        //    bundle has not been finished. We have to remember to manually
        //    finish the bundle in case we receive the final watermark.
        //    To avoid latency, we should process this watermark again as
        //    soon as the current bundle is finished.
        //
        // Approach 1) is the easiest and gives better latency, yet 2)
        // gives better throughput due to the bundle not getting cut on
        // every watermark. So we have implemented 2) below.

        // advance the watermark and do not emit watermark to downstream operators
        if (getTimeServiceManager().isPresent()) {
            getTimeServiceManager().get().advanceWatermark(mark);
        }

        if (mark.getTimestamp() == Long.MAX_VALUE) {
            invokeFinishBundle();
            processElementsOfCurrentKeyIfNeeded(null);
            advanceWatermark(mark);
            output.emitWatermark(mark);
        } else if (isBundleFinished()) {
            output.emitWatermark(mark);
        } else {
            // It is not safe to advance the output watermark yet, so add a hold on the current
            // output watermark.
            bundleFinishedCallback =
                    () -> {
                        try {
                            advanceWatermark(mark);
                            // at this point the bundle is finished, allow the watermark to pass
                            output.emitWatermark(mark);
                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to process watermark after finished bundle.", e);
                        }
                    };
        }
    }

    @Override
    public void setCurrentKey(Object key) {
        processElementsOfCurrentKeyIfNeeded(key);
        super.setCurrentKey(key);
    }

    private void processElementsOfCurrentKeyIfNeeded(Object newKey) {
        // process all the elements belonging to the current key when encountering a new key
        // for batch operator
        if (getKeyedStateStore() != null
                && inBatchExecutionMode(getKeyedStateBackend())
                && !Objects.equals(newKey, getCurrentKey())) {
            while (!isBundleFinished()) {
                try {
                    invokeFinishBundle();
                    fireAllRegisteredTimers(newKey);
                } catch (Exception e) {
                    throw new WrappingRuntimeException(e);
                }
            }
        }
    }

    private void fireAllRegisteredTimers(Object newKey) throws Exception {
        Field keySelectionListenersField =
                BatchExecutionKeyedStateBackend.class.getDeclaredField("keySelectionListeners");
        keySelectionListenersField.setAccessible(true);
        List<KeyedStateBackend.KeySelectionListener<Object>> listeners =
                (List<KeyedStateBackend.KeySelectionListener<Object>>)
                        keySelectionListenersField.get(getKeyedStateBackend());
        for (KeyedStateBackend.KeySelectionListener<Object> listener : listeners) {
            if (listener instanceof BatchExecutionInternalTimeServiceManager) {
                // fire the registered timers
                listener.keySelected(newKey);

                // set back the current key
                listener.keySelected(getCurrentKey());
            }
        }
    }

    /** Returns whether the bundle is finished. */
    public boolean isBundleFinished() {
        return elementCount == 0;
    }

    /** Reset the {@link Configuration} if needed. */
    public void setConfiguration(Configuration config) {
        this.config = config;
    }

    /** Returns the {@link Configuration}. */
    public Configuration getConfiguration() {
        return config;
    }

    /**
     * Creates the {@link PythonFunctionRunner} which is responsible for Python user-defined
     * function execution.
     */
    public abstract PythonFunctionRunner createPythonFunctionRunner() throws Exception;

    /** Returns the {@link PythonEnv} used to create PythonEnvironmentManager.. */
    public abstract PythonEnv getPythonEnv();

    /** Sends the execution result to the downstream operator. */
    public abstract void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception;

    protected void emitResults() throws Exception {
        Tuple2<byte[], Integer> resultTuple;
        while ((resultTuple = pythonFunctionRunner.pollResult()) != null && resultTuple.f1 != 0) {
            emitResult(resultTuple);
        }
    }

    /**
     * Advances the watermark of all managed timer services, potentially firing event time timers.
     * It also ensures that the fired timers are processed in the Python user-defined functions.
     */
    private void advanceWatermark(Watermark watermark) throws Exception {
        if (getTimeServiceManager().isPresent()) {
            InternalTimeServiceManager<?> timeServiceManager = getTimeServiceManager().get();
            timeServiceManager.advanceWatermark(watermark);

            while (!isBundleFinished()) {
                invokeFinishBundle();
                timeServiceManager.advanceWatermark(watermark);
            }
        }
    }

    /** Checks whether to invoke finishBundle by elements count. Called in processElement. */
    protected void checkInvokeFinishBundleByCount() throws Exception {
        if (elementCount >= maxBundleSize) {
            invokeFinishBundle();
        }
    }

    /** Checks whether to invoke finishBundle by timeout. */
    private void checkInvokeFinishBundleByTime() throws Exception {
        long now = getProcessingTimeService().getCurrentProcessingTime();
        if (now - lastFinishBundleTime >= maxBundleTimeMills) {
            invokeFinishBundle();
        }
    }

    protected void invokeFinishBundle() throws Exception {
        if (elementCount > 0) {
            AtomicBoolean flushThreadFinish = new AtomicBoolean(false);
            AtomicReference<Throwable> exceptionReference = new AtomicReference<>();
            flushThreadPool.submit(
                    () -> {
                        try {
                            pythonFunctionRunner.flush();
                        } catch (Throwable e) {
                            exceptionReference.set(e);
                        } finally {
                            flushThreadFinish.set(true);
                            // interrupt the progress of takeResult to avoid the main thread is
                            // blocked forever.
                            ((BeamPythonFunctionRunner) pythonFunctionRunner).notifyNoMoreResults();
                        }
                    });
            Tuple2<byte[], Integer> resultTuple;
            while (!flushThreadFinish.get()) {
                resultTuple = pythonFunctionRunner.takeResult();
                if (resultTuple.f1 != 0) {
                    emitResult(resultTuple);
                    emitResults();
                }
            }
            emitResults();
            Throwable flushThreadThrowable = exceptionReference.get();
            if (flushThreadThrowable != null) {
                throw new RuntimeException(
                        "Error while waiting for BeamPythonFunctionRunner flush",
                        flushThreadThrowable);
            }
            elementCount = 0;
            lastFinishBundleTime = getProcessingTimeService().getCurrentProcessingTime();
            // callback only after current bundle was fully finalized
            if (bundleFinishedCallback != null) {
                bundleFinishedCallback.run();
                bundleFinishedCallback = null;
            }
        }
    }

    protected PythonEnvironmentManager createPythonEnvironmentManager() {
        PythonDependencyInfo dependencyInfo =
                PythonDependencyInfo.create(
                        pythonConfig, getRuntimeContext().getDistributedCache());
        PythonEnv pythonEnv = getPythonEnv();
        if (pythonEnv.getExecType() == PythonEnv.ExecType.PROCESS) {
            return new ProcessPythonEnvironmentManager(
                    dependencyInfo,
                    getContainingTask().getEnvironment().getTaskManagerInfo().getTmpDirectories(),
                    new HashMap<>(System.getenv()),
                    getRuntimeContext().getJobId());
        } else {
            throw new UnsupportedOperationException(
                    String.format(
                            "Execution type '%s' is not supported.", pythonEnv.getExecType()));
        }
    }

    protected FlinkMetricContainer getFlinkMetricContainer() {
        return this.pythonConfig.isMetricEnabled()
                ? new FlinkMetricContainer(getRuntimeContext().getMetricGroup())
                : null;
    }
}
