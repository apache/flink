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

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class FlinkKafkaConsumer09WithPunctuatedWM<T> extends FlinkKafkaConsumer09Base<T> {

	/**
	 * The user-specified methods to extract the timestamps from the records in Kafka, and
	 * to decide when to emit watermarks.
	 */
	private final AssignerWithPunctuatedWatermarks<T> punctuatedWatermarkAssigner;

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 * @param timestampAssigner
	 *           The user-specified methods to extract the timestamps and decide when to emit watermarks.
	 *           This has to implement the {@link AssignerWithPunctuatedWatermarks} interface.
	 */
	public FlinkKafkaConsumer09WithPunctuatedWM(String topic, DeserializationSchema<T> valueDeserializer, Properties props,
												AssignerWithPunctuatedWatermarks<T> timestampAssigner) {
		this(Collections.singletonList(topic), valueDeserializer, props, timestampAssigner);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
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
	 * @param timestampAssigner
	 *           The user-specified methods to extract the timestamps and decide when to emit watermarks.
	 *           This has to implement the {@link AssignerWithPunctuatedWatermarks} interface.
	 */
	public FlinkKafkaConsumer09WithPunctuatedWM(String topic, KeyedDeserializationSchema<T> deserializer, Properties props,
												AssignerWithPunctuatedWatermarks<T> timestampAssigner) {
		this(Collections.singletonList(topic), deserializer, props, timestampAssigner);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
	 *
	 * This constructor allows passing multiple topics to the consumer.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 * @param timestampAssigner
	 *           The user-specified methods to extract the timestamps and decide when to emit watermarks.
	 *           This has to implement the {@link AssignerWithPunctuatedWatermarks} interface.
	 */
	public FlinkKafkaConsumer09WithPunctuatedWM(List<String> topics, DeserializationSchema<T> deserializer, Properties props,
												AssignerWithPunctuatedWatermarks<T> timestampAssigner) {
		this(topics, new KeyedDeserializationSchemaWrapper<>(deserializer), props, timestampAssigner);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
	 *
	 * This constructor allows passing multiple topics and a key/value deserialization schema.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 * @param timestampAssigner
	 *           The user-specified methods to extract the timestamps and decide when to emit watermarks.
	 *           This has to implement the {@link AssignerWithPunctuatedWatermarks} interface.
	 */
	public FlinkKafkaConsumer09WithPunctuatedWM(List<String> topics, KeyedDeserializationSchema<T> deserializer, Properties props,
												AssignerWithPunctuatedWatermarks<T> timestampAssigner) {
		super(topics, deserializer, props);
		this.punctuatedWatermarkAssigner = timestampAssigner;
	}

	@Override
	public void processElement(SourceContext<T> sourceContext, String topic, int partition, T value) {
		// extract the timestamp based on the user-specified extractor
		// emits the element with the new timestamp
		// updates the list of minimum timestamps seen per topic per partition (if necessary)
		// gets the minimum timestamp across all topics and all local partitions
		// if it is time to emit a watermark (based on the user-specified function, it sends a watermark
		// containing the minimum timestamp seen so far, across all local partitions for a given topic

		// todo when it terminates and there are partitions with no more elements???

		long extractedTimestamp = punctuatedWatermarkAssigner.extractTimestamp(value, Long.MIN_VALUE);
		sourceContext.collectWithTimestamp(value, extractedTimestamp);

		updateMinimumTimestampForPartition(topic, partition, extractedTimestamp);
		long minTimestamp = getMinimumTimestampAcrossAllTopics();

		final Watermark nextWatermark = punctuatedWatermarkAssigner
			.checkAndGetNextWatermark(value, extractedTimestamp);

		if (nextWatermark != null) {
			emitWatermarkIfMarkingProgress(sourceContext, minTimestamp);
		}
	}
}
