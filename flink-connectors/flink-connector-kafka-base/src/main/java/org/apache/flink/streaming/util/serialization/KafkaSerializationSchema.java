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

package org.apache.flink.streaming.util.serialization;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * The serialization schema describes how to turn a data object into a {@link ProducerRecord}.
 *
 * @param <T> The type to be serialized.
 */
public interface KafkaSerializationSchema<T> extends Serializable {
	/**
	 * Serializes given element into ProducerRecord.
	 * @param element element to be serialized
	 * @param timestamp timestamp (can be null)
	 * @param partitionInfo - extra information, which may be needed for partitiong
	 * @return Kafka ProducerRecord
	 */
	ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp, PartitionInfo partitionInfo);

	/**
	 * Partitions information, to allow serialize method calculate target partition.
	 */
	interface PartitionInfo {
		/**
		 * Return array of partitions for given {@code topicName}.
		 *
		 * @param topicName name of topic for which partitions are requested
		 * @return array of partitions.
		 */
		int[] partitionsFor(String topicName);

		/**
		 * Gets the number of the parallel subtask. The numbering starts from 0 and goes up to
		 * parallelism-1 (parallelism as returned by {@link #getMaxNumberOfParallelSubtasks()}).
		 *
		 * @return The index of the parallel subtask.
		 */
		int getIndexOfThisSubtask();

		/**
		 * Gets the number of max-parallelism with which the parallel task runs.
		 *
		 * @return The max-parallelism with which the parallel task runs.
		 */
		int getMaxNumberOfParallelSubtasks();
	}
}
