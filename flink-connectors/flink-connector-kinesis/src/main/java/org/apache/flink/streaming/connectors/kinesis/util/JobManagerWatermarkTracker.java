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

package org.apache.flink.streaming.connectors.kinesis.util;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.taskexecutor.GlobalAggregateManager;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/** A {@link WatermarkTracker} that shares state through {@link GlobalAggregateManager}. */
@PublicEvolving
public class JobManagerWatermarkTracker extends WatermarkTracker {

    private GlobalAggregateManager aggregateManager;
    private final String aggregateName;
    private final WatermarkAggregateFunction aggregateFunction = new WatermarkAggregateFunction();
    private final long logAccumulatorIntervalMillis;
    private long updateTimeoutCount;

    public JobManagerWatermarkTracker(String aggregateName) {
        this(aggregateName, -1);
    }

    public JobManagerWatermarkTracker(String aggregateName, long logAccumulatorIntervalMillis) {
        super();
        this.aggregateName = aggregateName;
        this.logAccumulatorIntervalMillis = logAccumulatorIntervalMillis;
    }

    @Override
    public long updateWatermark(long localWatermark) {
        WatermarkUpdate update = new WatermarkUpdate();
        update.id = getSubtaskId();
        update.watermark = localWatermark;
        try {
            byte[] resultBytes =
                    aggregateManager.updateGlobalAggregate(
                            aggregateName,
                            InstantiationUtil.serializeObject(update),
                            aggregateFunction);
            WatermarkResult result =
                    InstantiationUtil.deserializeObject(
                            resultBytes, this.getClass().getClassLoader());
            this.updateTimeoutCount += result.updateTimeoutCount;
            return result.watermark;
        } catch (ClassNotFoundException | IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public void open(RuntimeContext context) {
        super.open(context);
        this.aggregateFunction.updateTimeoutMillis = super.getUpdateTimeoutMillis();
        this.aggregateFunction.logAccumulatorIntervalMillis = logAccumulatorIntervalMillis;
        Preconditions.checkArgument(context instanceof StreamingRuntimeContext);
        StreamingRuntimeContext runtimeContext = (StreamingRuntimeContext) context;
        this.aggregateManager = runtimeContext.getGlobalAggregateManager();
    }

    public long getUpdateTimeoutCount() {
        return updateTimeoutCount;
    }

    /** Watermark aggregation input. */
    protected static class WatermarkUpdate implements Serializable {
        protected long watermark = Long.MIN_VALUE;
        protected String id;
    }

    /** Watermark aggregation result. */
    protected static class WatermarkResult implements Serializable {
        protected long watermark = Long.MIN_VALUE;
        protected long updateTimeoutCount = 0;
    }

    /** Aggregate function for computing a combined watermark of parallel subtasks. */
    private static class WatermarkAggregateFunction
            implements AggregateFunction<byte[], Map<String, WatermarkState>, byte[]> {

        private long updateTimeoutMillis = DEFAULT_UPDATE_TIMEOUT_MILLIS;
        private long logAccumulatorIntervalMillis = -1;

        // TODO: wrap accumulator
        static long addCount;
        static long lastLogged;
        private static final Logger LOG = LoggerFactory.getLogger(WatermarkAggregateFunction.class);

        @Override
        public Map<String, WatermarkState> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public Map<String, WatermarkState> add(
                byte[] valueBytes, Map<String, WatermarkState> accumulator) {
            addCount++;
            final WatermarkUpdate value;
            try {
                value =
                        InstantiationUtil.deserializeObject(
                                valueBytes, this.getClass().getClassLoader());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            WatermarkState ws = accumulator.get(value.id);
            if (ws == null) {
                accumulator.put(value.id, ws = new WatermarkState());
            }
            ws.watermark = value.watermark;
            ws.lastUpdated = System.currentTimeMillis();
            return accumulator;
        }

        @Override
        public byte[] getResult(Map<String, WatermarkState> accumulator) {
            long updateTimeoutCount = 0;
            long currentTime = System.currentTimeMillis();
            long globalWatermark = Long.MAX_VALUE;
            for (Map.Entry<String, WatermarkState> e : accumulator.entrySet()) {
                WatermarkState ws = e.getValue();
                if (ws.lastUpdated + updateTimeoutMillis < currentTime) {
                    // ignore outdated entry
                    if (ws.watermark < Long.MAX_VALUE) {
                        updateTimeoutCount++;
                    }
                    continue;
                }
                globalWatermark = Math.min(ws.watermark, globalWatermark);
            }

            WatermarkResult result = new WatermarkResult();
            result.watermark = globalWatermark == Long.MAX_VALUE ? Long.MIN_VALUE : globalWatermark;
            result.updateTimeoutCount = updateTimeoutCount;

            if (logAccumulatorIntervalMillis > 0) {
                if (currentTime - lastLogged > logAccumulatorIntervalMillis) {
                    lastLogged = System.currentTimeMillis();
                    LOG.info(
                            "WatermarkAggregateFunction added: {}, timeout: {}, map: {}",
                            addCount,
                            updateTimeoutCount,
                            accumulator);
                }
            }

            try {
                return InstantiationUtil.serializeObject(result);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Map<String, WatermarkState> merge(
                Map<String, WatermarkState> accumulatorA,
                Map<String, WatermarkState> accumulatorB) {
            // not required
            throw new UnsupportedOperationException();
        }
    }
}
