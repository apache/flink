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

package org.apache.flink.streaming.runtime.io.recovery;

import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link VirtualChannelRecordFilterFactory}. */
class VirtualChannelRecordFilterFactoryTest {

    @Test
    void testCreatePassThroughFilter() {
        RecordFilter<Long> filter = VirtualChannelRecordFilterFactory.createPassThroughFilter();
        assertThat(filter.filter(new StreamRecord<>(0L))).isTrue();
        assertThat(filter.filter(new StreamRecord<>(1L))).isTrue();
        assertThat(filter.filter(new StreamRecord<>(42L))).isTrue();
    }

    @Test
    void testCreateFilterProducesPartitionerBasedFilter() {
        RebalancePartitioner<Long> partitioner = new RebalancePartitioner<>();

        VirtualChannelRecordFilterFactory<Long> factory =
                new VirtualChannelRecordFilterFactory<>(
                        LongSerializer.INSTANCE, partitioner, 0, 2, 128);

        RecordFilter<Long> filter = factory.createFilter();
        // The filter should be a PartitionerRecordFilter that filters based on partitioner
        assertThat(filter).isInstanceOf(PartitionerRecordFilter.class);
    }

    @Test
    void testFromContextCreatesFactory() {
        RebalancePartitioner<Long> partitioner = new RebalancePartitioner<>();
        RecordFilterContext.InputFilterConfig config =
                new RecordFilterContext.InputFilterConfig(LongSerializer.INSTANCE, partitioner, 4);

        RecordFilterContext context =
                new RecordFilterContext(
                        new RecordFilterContext.InputFilterConfig[] {config},
                        InflightDataRescalingDescriptor.NO_RESCALE,
                        1,
                        128,
                        new String[] {"/tmp"},
                        true);

        VirtualChannelRecordFilterFactory<Long> factory =
                VirtualChannelRecordFilterFactory.fromContext(context, 0);
        RecordFilter<Long> filter = factory.createFilter();

        // The filter should be a functional PartitionerRecordFilter
        assertThat(filter).isInstanceOf(PartitionerRecordFilter.class);
    }

    @Test
    void testEachFilterCallCreatesIndependentFilter() {
        RebalancePartitioner<Long> partitioner = new RebalancePartitioner<>();

        VirtualChannelRecordFilterFactory<Long> factory =
                new VirtualChannelRecordFilterFactory<>(
                        LongSerializer.INSTANCE, partitioner, 0, 2, 128);

        RecordFilter<Long> filter1 = factory.createFilter();
        RecordFilter<Long> filter2 = factory.createFilter();

        // Each call should produce a distinct filter instance (using a copy of the partitioner)
        assertThat(filter1).isNotSameAs(filter2);
    }
}
