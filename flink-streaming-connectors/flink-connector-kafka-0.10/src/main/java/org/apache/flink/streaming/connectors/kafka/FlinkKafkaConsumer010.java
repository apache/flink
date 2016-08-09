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

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internal.Kafka010Fetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.util.SerializedValue;

import java.util.Collections;
import java.util.List;
import java.util.Properties;


/**
 * The Flink Kafka Consumer is a streaming data source that pulls a parallel data stream from
 * Apache Kafka 0.10.x. The consumer can run in multiple parallel instances, each of which will pull
 * data from one or more Kafka partitions. 
 * 
 * <p>The Flink Kafka Consumer participates in checkpointing and guarantees that no data is lost
 * during a failure, and that the computation processes elements "exactly once". 
 * (Note: These guarantees naturally assume that Kafka itself does not loose any data.)</p>
 *
 * <p>Please note that Flink snapshots the offsets internally as part of its distributed checkpoints. The offsets
 * committed to Kafka / ZooKeeper are only to bring the outside view of progress in sync with Flink's view
 * of the progress. That way, monitoring and other jobs can get a view of how far the Flink Kafka consumer
 * has consumed a topic.</p>
 *
 * <p>Please refer to Kafka's documentation for the available configuration properties:
 * http://kafka.apache.org/documentation.html#newconsumerconfigs</p>
 *
 * <p><b>NOTE:</b> The implementation currently accesses partition metadata when the consumer
 * is constructed. That means that the client that submits the program needs to be able to
 * reach the Kafka brokers or ZooKeeper.</p>
 */
public class FlinkKafkaConsumer010<T> extends FlinkKafkaConsumer09<T> {

	private static final long serialVersionUID = 2324564345203409112L;


	// ------------------------------------------------------------------------

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer010(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
		this(Collections.singletonList(topic), valueDeserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x
	 *
	 * This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from Kafka.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer010(String topic, KeyedDeserializationSchema<T> deserializer, Properties props) {
		this(Collections.singletonList(topic), deserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x
	 *
	 * This constructor allows passing multiple topics to the consumer.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer010(List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
		this(topics, new KeyedDeserializationSchemaWrapper<>(deserializer), props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.10.x
	 *
	 * This constructor allows passing multiple topics and a key/value deserialization schema.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer010(List<String> topics, KeyedDeserializationSchema<T> deserializer, Properties props) {
		super(topics, deserializer, props);
	}

	@Override
	protected AbstractFetcher<T, ?> createFetcher(
			SourceContext<T> sourceContext,
			List<KafkaTopicPartition> thisSubtaskPartitions,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext) throws Exception {

		boolean useMetrics = !Boolean.valueOf(properties.getProperty(KEY_DISABLE_METRICS, "false"));

		return new Kafka010Fetcher<>(sourceContext, thisSubtaskPartitions,
				watermarksPeriodic, watermarksPunctuated,
				runtimeContext, deserializer,
				properties, pollTimeout, useMetrics);
	}
}
