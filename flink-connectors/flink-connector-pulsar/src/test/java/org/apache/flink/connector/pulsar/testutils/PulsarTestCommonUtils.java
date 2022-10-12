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

package org.apache.flink.connector.pulsar.testutils;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.streaming.api.CheckpointingMode;

import org.apache.pulsar.client.api.MessageId;
import org.junit.jupiter.api.extension.ParameterContext;

import java.util.ArrayList;
import java.util.List;

/** Put static methods that can be used by multiple test classes. */
public class PulsarTestCommonUtils {

    /** Convert the CheckpointingMode to a connector related DeliveryGuarantee. */
    public static DeliveryGuarantee toDeliveryGuarantee(CheckpointingMode checkpointingMode) {
        if (checkpointingMode == CheckpointingMode.AT_LEAST_ONCE) {
            return DeliveryGuarantee.AT_LEAST_ONCE;
        } else if (checkpointingMode == CheckpointingMode.EXACTLY_ONCE) {
            return DeliveryGuarantee.EXACTLY_ONCE;
        } else {
            throw new IllegalArgumentException(
                    "Only exactly-once and al-least-once checkpointing mode are supported.");
        }
    }

    /** creates a fullRange() partitionSplit. */
    public static PulsarPartitionSplit createPartitionSplit(String topic, int partitionId) {
        return createPartitionSplit(topic, partitionId, Boundedness.CONTINUOUS_UNBOUNDED);
    }

    public static PulsarPartitionSplit createPartitionSplit(
            String topic, int partitionId, Boundedness boundedness) {
        return createPartitionSplit(topic, partitionId, boundedness, MessageId.earliest);
    }

    public static PulsarPartitionSplit createPartitionSplit(
            String topic, int partitionId, Boundedness boundedness, MessageId latestConsumedId) {
        TopicPartition topicPartition = new TopicPartition(topic, partitionId);
        StopCursor stopCursor =
                boundedness == Boundedness.BOUNDED ? StopCursor.latest() : StopCursor.never();
        return new PulsarPartitionSplit(topicPartition, stopCursor, latestConsumedId, null);
    }

    public static List<PulsarPartitionSplit> createPartitionSplits(
            String topicName, int numSplits, Boundedness boundedness) {
        List<PulsarPartitionSplit> splits = new ArrayList<>();
        for (int i = 0; i < numSplits; i++) {
            splits.add(createPartitionSplit(topicName, i, boundedness));
        }
        return splits;
    }

    public static boolean isAssignableFromParameterContext(
            Class<?> requiredType, ParameterContext context) {
        return requiredType.isAssignableFrom(context.getParameter().getType());
    }
}
