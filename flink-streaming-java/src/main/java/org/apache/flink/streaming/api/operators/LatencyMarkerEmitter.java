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
import org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.concurrent.ScheduledFuture;

/** A small helper class to periodically emit {@link LatencyMarker}. */
@Internal
class LatencyMarkerEmitter<OUT> {
    private final ScheduledFuture<?> latencyMarkTimer;

    public LatencyMarkerEmitter(
            ProcessingTimeService processingTimeService,
            EmitAction emitAction,
            long latencyTrackingInterval,
            OperatorID operatorId,
            int subtaskIndex) {
        latencyMarkTimer =
                processingTimeService.scheduleWithFixedDelay(
                        new ProcessingTimeCallback() {
                            @Override
                            public void onProcessingTime(long timestamp) {
                                try {
                                    emitAction.emitLatencyMarker(
                                            new LatencyMarker(
                                                    processingTimeService
                                                            .getCurrentProcessingTime(),
                                                    operatorId,
                                                    subtaskIndex));
                                } catch (Throwable t) {
                                    // we catch the Throwables here so that we don't trigger the
                                    // processing
                                    // timer services async exception handler
                                    AbstractStreamOperator.LOG.warn(
                                            "Error while emitting latency marker.", t);
                                }
                            }
                        },
                        0L,
                        latencyTrackingInterval);
    }

    public void close() {
        latencyMarkTimer.cancel(true);
    }

    interface EmitAction {
        void emitLatencyMarker(LatencyMarker latencyMarker) throws Exception;
    }
}
