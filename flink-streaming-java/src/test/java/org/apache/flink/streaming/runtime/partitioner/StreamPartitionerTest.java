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

package org.apache.flink.streaming.runtime.partitioner;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for different {@link StreamPartitioner} implementations. */
abstract class StreamPartitionerTest {

    final StreamPartitioner<Tuple> streamPartitioner = createPartitioner();
    final StreamRecord<Tuple> streamRecord = new StreamRecord<>(null);
    final SerializationDelegate<StreamRecord<Tuple>> serializationDelegate =
            new SerializationDelegate<>(null);

    abstract StreamPartitioner<Tuple> createPartitioner();

    @BeforeEach
    void setup() {
        serializationDelegate.setInstance(streamRecord);
    }

    void assertSelectedChannel(int expectedChannel) {
        int actualResult = streamPartitioner.selectChannel(serializationDelegate);
        assertThat(actualResult).isEqualTo(expectedChannel);
    }

    void assertSelectedChannelWithSetup(int expectedChannel, int numberOfChannels) {
        streamPartitioner.setup(numberOfChannels);
        assertSelectedChannel(expectedChannel);
    }

    @Test
    void testSerializable() throws IOException, ClassNotFoundException {
        final StreamPartitioner<Tuple> clone = InstantiationUtil.clone(streamPartitioner);
        assertThat(clone).isEqualTo(streamPartitioner);
    }
}
