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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;

/**
 * {@link StreamOperator} for streaming sources.
 *
 * @param <OUT> Type of the output elements
 * @param <SRC> Type of the source function of this stream source operator
 */
@Internal
public class StreamSource<OUT, SRC extends SourceFunction<OUT>>
        extends AbstractUdfStreamOperator<OUT, SRC> {

    private static final long serialVersionUID = 1L;

    /** Whether to emit intermediate watermarks or only one final watermark at the end of input. */
    private final boolean emitProgressiveWatermarks;

    private transient SourceFunction.SourceContext<OUT> ctx;

    private transient volatile boolean canceledOrStopped = false;

    public StreamSource(SRC sourceFunction, boolean emitProgressiveWatermarks) {
        super(sourceFunction);

        this.chainingStrategy = ChainingStrategy.HEAD;
        this.emitProgressiveWatermarks = emitProgressiveWatermarks;
    }

    public StreamSource(SRC sourceFunction) {
        this(sourceFunction, true);
    }

    @VisibleForTesting
    public boolean emitsProgressiveWatermarks() {
        return emitProgressiveWatermarks;
    }

    public void run(final Object lockingObject, final OperatorChain<?, ?> operatorChain)
            throws Exception {

        run(lockingObject, output, operatorChain);
    }

    public void run(
            final Object lockingObject,
            final Output<StreamRecord<OUT>> collector,
            final OperatorChain<?, ?> operatorChain)
            throws Exception {

        final TimeCharacteristic timeCharacteristic = getOperatorConfig().getTimeCharacteristic();

        final Configuration configuration =
                this.getContainingTask().getEnvironment().getTaskManagerInfo().getConfiguration();
        final long latencyTrackingInterval =
                getExecutionConfig().isLatencyTrackingConfigured()
                        ? getExecutionConfig().getLatencyTrackingInterval()
                        : configuration.getLong(MetricOptions.LATENCY_INTERVAL);

        LatencyMarkerEmitter<OUT> latencyEmitter = null;
        if (latencyTrackingInterval > 0) {
            latencyEmitter =
                    new LatencyMarkerEmitter<>(
                            getProcessingTimeService(),
                            collector::emitLatencyMarker,
                            latencyTrackingInterval,
                            this.getOperatorID(),
                            getRuntimeContext().getIndexOfThisSubtask());
        }

        final long watermarkInterval =
                getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();

        this.ctx =
                StreamSourceContexts.getSourceContext(
                        timeCharacteristic,
                        getProcessingTimeService(),
                        lockingObject,
                        collector,
                        watermarkInterval,
                        -1,
                        emitProgressiveWatermarks);

        try {
            userFunction.run(ctx);
        } finally {
            if (latencyEmitter != null) {
                latencyEmitter.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        // make sure that the context is closed in any case
        if (ctx != null) {
            ctx.close();
        }
        super.close();
    }

    public void stop() {
        userFunction.cancel();
    }

    public void cancel() {
        // important: marking the source as stopped has to happen before the function is stopped.
        // the flag that tracks this status is volatile, so the memory model also guarantees
        // the happens-before relationship
        markCanceledOrStopped();
        userFunction.cancel();

        // the context may not be initialized if the source was never running.
        if (ctx != null) {
            ctx.close();
        }
    }

    /**
     * Marks this source as canceled or stopped.
     *
     * <p>This indicates that any exit of the {@link #run(Object, Output, OperatorChain)} method
     * cannot be interpreted as the result of a finite source.
     */
    protected void markCanceledOrStopped() {
        this.canceledOrStopped = true;
    }

    /**
     * Checks whether the source has been canceled or stopped.
     *
     * @return True, if the source is canceled or stopped, false is not.
     */
    protected boolean isCanceledOrStopped() {
        return canceledOrStopped;
    }
}
