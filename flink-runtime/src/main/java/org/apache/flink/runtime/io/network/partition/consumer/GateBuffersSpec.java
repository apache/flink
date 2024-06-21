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

package org.apache.flink.runtime.io.network.partition.consumer;

/**
 * The buffer specs of the {@link InputGate} include exclusive buffers per channel, required/total
 * floating buffers and the target of total buffers.
 */
public class GateBuffersSpec {

    private final int effectiveExclusiveBuffersPerChannel;

    private final int requiredFloatingBuffers;

    private final int totalFloatingBuffers;

    private final int targetTotalBuffersPerGate;

    GateBuffersSpec(
            int effectiveExclusiveBuffersPerChannel,
            int requiredFloatingBuffers,
            int totalFloatingBuffers,
            int targetTotalBuffersPerGate) {
        this.effectiveExclusiveBuffersPerChannel = effectiveExclusiveBuffersPerChannel;
        this.requiredFloatingBuffers = requiredFloatingBuffers;
        this.totalFloatingBuffers = totalFloatingBuffers;
        this.targetTotalBuffersPerGate = targetTotalBuffersPerGate;
    }

    int getRequiredFloatingBuffers() {
        return requiredFloatingBuffers;
    }

    int getTotalFloatingBuffers() {
        return totalFloatingBuffers;
    }

    int getEffectiveExclusiveBuffersPerChannel() {
        return effectiveExclusiveBuffersPerChannel;
    }

    public int targetTotalBuffersPerGate() {
        return targetTotalBuffersPerGate;
    }
}
