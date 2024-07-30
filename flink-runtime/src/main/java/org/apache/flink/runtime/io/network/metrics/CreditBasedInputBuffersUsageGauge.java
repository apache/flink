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

package org.apache.flink.runtime.io.network.metrics;

import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Gauge metric measuring the input buffers usage for {@link SingleInputGate}s under credit based
 * mode.
 */
public class CreditBasedInputBuffersUsageGauge extends AbstractBuffersUsageGauge {

    private final FloatingBuffersUsageGauge floatingBuffersUsageGauge;
    private final ExclusiveBuffersUsageGauge exclusiveBuffersUsageGauge;

    public CreditBasedInputBuffersUsageGauge(
            FloatingBuffersUsageGauge floatingBuffersUsageGauge,
            ExclusiveBuffersUsageGauge exclusiveBuffersUsageGauge,
            SingleInputGate[] inputGates) {
        super(checkNotNull(inputGates));
        this.floatingBuffersUsageGauge = checkNotNull(floatingBuffersUsageGauge);
        this.exclusiveBuffersUsageGauge = checkNotNull(exclusiveBuffersUsageGauge);
    }

    @Override
    public int calculateUsedBuffers(SingleInputGate inputGate) {
        return floatingBuffersUsageGauge.calculateUsedBuffers(inputGate)
                + exclusiveBuffersUsageGauge.calculateUsedBuffers(inputGate);
    }

    @Override
    public int calculateTotalBuffers(SingleInputGate inputGate) {
        return floatingBuffersUsageGauge.calculateTotalBuffers(inputGate)
                + exclusiveBuffersUsageGauge.calculateTotalBuffers(inputGate);
    }
}
