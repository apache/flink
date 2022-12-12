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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.TestingConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link SingleInputGateFactory.GateBuffersNumCalculator}. */
class GateBuffersNumCalculatorTest {

    @Test
    void testCalculationWhenNotUseFloating() {
        int numInputChannels = 500;
        SingleInputGateFactory.GateBuffersNumCalculator gateBuffersNumCalculator =
                createGateBuffersNumCalculator(numInputChannels);

        assertThat(gateBuffersNumCalculator.getMinFloatingBuffers()).isEqualTo(1);
        assertThat(gateBuffersNumCalculator.getMaxFloatingBuffers()).isEqualTo(8);
        assertThat(gateBuffersNumCalculator.getExclusiveBuffersPerChannel()).isEqualTo(2);
    }

    @Test
    void testCalculationWhenDecreaseExclusiveBuffers() {
        int numInputChannels = 501;
        SingleInputGateFactory.GateBuffersNumCalculator gateBuffersNumCalculator =
                createGateBuffersNumCalculator(numInputChannels);

        assertThat(gateBuffersNumCalculator.getMinFloatingBuffers()).isEqualTo(1);
        assertThat(gateBuffersNumCalculator.getMaxFloatingBuffers()).isEqualTo(8);
        assertThat(gateBuffersNumCalculator.getExclusiveBuffersPerChannel()).isEqualTo(1);
    }

    @Test
    void testBoundaryCalculationWhenDecreaseExclusive() {
        int numInputChannels = 1000;
        SingleInputGateFactory.GateBuffersNumCalculator gateBuffersNumCalculator =
                createGateBuffersNumCalculator(numInputChannels);

        assertThat(gateBuffersNumCalculator.getMinFloatingBuffers()).isEqualTo(1);
        assertThat(gateBuffersNumCalculator.getMaxFloatingBuffers()).isEqualTo(8);
        assertThat(gateBuffersNumCalculator.getExclusiveBuffersPerChannel()).isEqualTo(1);
    }

    @Test
    void testCalculationWhenUseAllFloatingBuffers() {
        int numInputChannels = 1001;
        SingleInputGateFactory.GateBuffersNumCalculator gateBuffersNumCalculator =
                createGateBuffersNumCalculator(numInputChannels);

        assertThat(gateBuffersNumCalculator.getMinFloatingBuffers()).isEqualTo(1000);
        assertThat(gateBuffersNumCalculator.getMaxFloatingBuffers())
                .isEqualTo(numInputChannels * 2 + 8);
        assertThat(gateBuffersNumCalculator.getExclusiveBuffersPerChannel()).isEqualTo(0);
    }

    private SingleInputGateFactory.GateBuffersNumCalculator createGateBuffersNumCalculator(
            int numInputChannels) {
        SingleInputGateFactory factory = createSingleInputGateFactory();
        ResultPartitionType partitionType = ResultPartitionType.BLOCKING;
        return factory.new GateBuffersNumCalculator(partitionType, numInputChannels);
    }

    private SingleInputGateFactory createSingleInputGateFactory() {
        NettyShuffleEnvironment netEnv = new NettyShuffleEnvironmentBuilder().build();
        return new SingleInputGateFactory(
                ResourceID.generate(),
                netEnv.getConfiguration(),
                new TestingConnectionManager(),
                netEnv.getResultPartitionManager(),
                new TaskEventDispatcher(),
                netEnv.getNetworkBufferPool());
    }
}
