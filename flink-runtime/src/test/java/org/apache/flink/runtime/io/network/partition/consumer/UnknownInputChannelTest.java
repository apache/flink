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

import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link UnknownInputChannel}. */
class UnknownInputChannelTest {
    @Test
    void testMetrics() {
        SingleInputGateBuilder builder =
                new SingleInputGateBuilder()
                        .setNumberOfChannels(1)
                        .setSingleInputGateIndex(0)
                        .setResultPartitionType(ResultPartitionType.PIPELINED);

        InputChannelMetrics metrics = new InputChannelMetrics(new UnregisteredMetricsGroup());
        UnknownInputChannel unknownInputChannel =
                InputChannelBuilder.newBuilder()
                        .setMetrics(metrics)
                        .buildUnknownChannel(builder.build());
        metrics.getNumBuffersInLocalCounter().inc();
        LocalInputChannel localInputChannel =
                unknownInputChannel.toLocalInputChannel(new ResultPartitionID());
        assertThat(localInputChannel.numBuffersIn.getCount())
                .isEqualTo(metrics.getNumBuffersInLocalCounter().getCount());
    }
}
