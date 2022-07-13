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

package org.apache.flink.connector.kafka.source.split;

import org.apache.kafka.common.TopicPartition;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KafkaPartitionSplitSerializer}. */
public class KafkaPartitionSplitSerializerTest {

    @Test
    public void testSerializer() throws IOException {
        String topic = "topic";
        Long offsetZero = 0L;
        Long normalOffset = 1L;
        TopicPartition topicPartition = new TopicPartition(topic, 1);
        List<Long> stoppingOffsets =
                Lists.newArrayList(
                        KafkaPartitionSplit.COMMITTED_OFFSET,
                        KafkaPartitionSplit.LATEST_OFFSET,
                        offsetZero,
                        normalOffset);
        KafkaPartitionSplitSerializer splitSerializer = new KafkaPartitionSplitSerializer();
        for (Long stoppingOffset : stoppingOffsets) {
            KafkaPartitionSplit kafkaPartitionSplit =
                    new KafkaPartitionSplit(topicPartition, 0, stoppingOffset);
            byte[] serialize = splitSerializer.serialize(kafkaPartitionSplit);
            KafkaPartitionSplit deserializeSplit =
                    splitSerializer.deserialize(splitSerializer.getVersion(), serialize);
            assertThat(deserializeSplit).isEqualTo(kafkaPartitionSplit);
        }
    }
}
