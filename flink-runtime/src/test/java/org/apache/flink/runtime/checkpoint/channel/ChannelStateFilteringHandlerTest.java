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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.streaming.runtime.io.recovery.RecordFilterContext;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ChannelStateFilteringHandler}. */
class ChannelStateFilteringHandlerTest {

    @TempDir Path tempDir;

    @Test
    void testCreateFromContextUsesProvidedSpillDirectories() {
        InputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(1).build();
        RecordFilterContext context = createRecordFilterContext(new String[] {tempDir.toString()});

        ChannelStateFilteringHandler handler =
                ChannelStateFilteringHandler.createFromContext(
                        context, new InputGate[] {inputGate});

        assertThat(handler).isNotNull();
        handler.close();
    }

    private static RecordFilterContext createRecordFilterContext(String[] tmpDirectories) {
        return new RecordFilterContext(
                new RecordFilterContext.InputFilterConfig[] {
                    new RecordFilterContext.InputFilterConfig(
                            LongSerializer.INSTANCE, new ForwardPartitioner<>(), 1)
                },
                new InflightDataRescalingDescriptor(
                        new InflightDataRescalingDescriptor
                                        .InflightDataGateOrPartitionRescalingDescriptor[] {
                            new InflightDataRescalingDescriptor
                                    .InflightDataGateOrPartitionRescalingDescriptor(
                                    new int[] {0},
                                    RescaleMappings.identity(1, 1),
                                    new HashSet<>(),
                                    InflightDataRescalingDescriptor
                                            .InflightDataGateOrPartitionRescalingDescriptor
                                            .MappingType.IDENTITY)
                        }),
                0,
                128,
                tmpDirectories,
                true,
                MemoryManager.DEFAULT_PAGE_SIZE);
    }
}
