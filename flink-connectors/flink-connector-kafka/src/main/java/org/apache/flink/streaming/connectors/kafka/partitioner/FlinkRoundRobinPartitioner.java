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

package org.apache.flink.streaming.connectors.kafka.partitioner;

import org.apache.flink.util.Preconditions;

import org.apache.kafka.common.utils.Utils;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A "Round-Robin" partitioner that can be used when user wants to distribute the writes to all
 * partitions equally.
 *
 * <p>This is the behaviour regardless of record key hash.
 */
public class FlinkRoundRobinPartitioner<T> extends FlinkKafkaPartitioner<T> {

    private ConcurrentMap<String, AtomicInteger> topicCounterMap;

    @Override
    public void open(int parallelInstanceId, int parallelInstances) {
        Preconditions.checkArgument(
                parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(
                parallelInstances > 0, "Number of subtasks must be larger than 0.");

        this.topicCounterMap = new ConcurrentHashMap();
    }

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(
                partitions != null && partitions.length > 0,
                "Partitions of the target topic is empty.");

        int numPartitions = partitions.length;
        int nextValue = this.nextValue(targetTopic);
        return Utils.toPositive(nextValue) % numPartitions;
    }

    private int nextValue(String topic) {
        AtomicInteger counter =
                this.topicCounterMap.computeIfAbsent(topic, (k) -> new AtomicInteger(0));
        return counter.getAndIncrement();
    }
}
