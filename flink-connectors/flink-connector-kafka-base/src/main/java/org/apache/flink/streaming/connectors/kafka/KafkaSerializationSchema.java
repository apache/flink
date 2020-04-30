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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.PublicEvolving;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A {@link KafkaSerializationSchema} defines how to serialize values of type {@code T} into {@link
 * ProducerRecord ProducerRecords}.
 *
 * <p>Please also implement {@link KafkaContextAware} if your serialization schema needs
 * information
 * about the available partitions and the number of parallel subtasks along with the subtask ID on
 * which the Kafka Producer is running.
 *
 * @param <T> the type of values being serialized
 */
@PublicEvolving
public interface KafkaSerializationSchema<T> extends Serializable {

	/**
	 * Serializes given element and returns it as a {@link ProducerRecord}.
	 *
	 * @param element element to be serialized
	 * @param timestamp timestamp (can be null)
	 * @return Kafka {@link ProducerRecord}
	 */
	ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp);
}
