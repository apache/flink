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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

import static java.util.Objects.requireNonNull;

/**
 * An implementation of a {@link FlinkKafkaComsumerWithWMBase} for Apache Kafka 0.9.x, that emits
 * watermarks periodically. The user has to provide a {@link AssignerWithPeriodicWatermarks}.
 * */
public class FlinkKafkaConsumer09WithPeriodicWM<T> extends FlinkKafkaConsumer09Base<T> implements Triggerable {

	/**
	 * The user-specified methods to extract the timestamps from the records in Kafka, and
	 * to decide when to emit watermarks.
	 */
	private final AssignerWithPeriodicWatermarks<T> periodicWatermarkAssigner;

	/**
	 * The interval between periodic watermark emissions, as configured via the
	 * {@link ExecutionConfig#getAutoWatermarkInterval()}.
	 */
	private long watermarkInterval = -1;

	private StreamingRuntimeContext runtime = null;

	private SourceContext<T> srcContext = null;

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
	 *           This has to implement the {@link AssignerWithPeriodicWatermarks} interface.
	 */
	public FlinkKafkaConsumer09WithPeriodicWM(String topic, DeserializationSchema<T> valueDeserializer, Properties props,
											AssignerWithPeriodicWatermarks<T> timestampAssigner) {
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
	 *           This has to implement the {@link AssignerWithPeriodicWatermarks} interface.
	 */
	public FlinkKafkaConsumer09WithPeriodicWM(String topic, KeyedDeserializationSchema<T> deserializer, Properties props,
											AssignerWithPeriodicWatermarks<T> timestampAssigner) {
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
	 *@param timestampAssigner
	 *           The user-specified methods to extract the timestamps and decide when to emit watermarks.
	 *           This has to implement the {@link AssignerWithPeriodicWatermarks} interface.
	 */
	public FlinkKafkaConsumer09WithPeriodicWM(List<String> topics, DeserializationSchema<T> deserializer, Properties props,
											AssignerWithPeriodicWatermarks<T> timestampAssigner) {
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
	 *           This has to implement the {@link AssignerWithPeriodicWatermarks} interface.
	 */
	public FlinkKafkaConsumer09WithPeriodicWM(List<String> topics, KeyedDeserializationSchema<T> deserializer, Properties props,
											AssignerWithPeriodicWatermarks<T> timestampAssigner) {
		super(topics, deserializer, props);
		this.periodicWatermarkAssigner = requireNonNull(timestampAssigner);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		if(runtime == null) {
			runtime = (StreamingRuntimeContext) getRuntimeContext();
		}

		watermarkInterval = getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval();
		if (watermarkInterval > 0) {
			runtime.registerTimer(System.currentTimeMillis() + watermarkInterval, this);
		}
	}

	@Override
	public void processElement(SourceContext<T> sourceContext, String topic, int partition, T value) {
		if(srcContext == null) {
			srcContext = sourceContext;
		}

		// extract the timestamp based on the user-specified extractor
		// emits the element with the new timestamp
		// updates the list of minimum timestamps seen per topic per partition (if necessary)

		long extractedTimestamp = periodicWatermarkAssigner.extractTimestamp(value, Long.MIN_VALUE);
		sourceContext.collectWithTimestamp(value, extractedTimestamp);
		updateMaximumTimestampForPartition(topic, partition, extractedTimestamp);
	}

	@Override
	public void trigger(long timestamp) throws Exception {
		if(this.srcContext == null) {
			// if the trigger is called before any elements, then we
			// just set the next timer to fire when it should and we
			// ignore the triggering as this would produce no results.

			setNextWatermarkTimer();
			return;
		}

		final Watermark nextWatermark = periodicWatermarkAssigner.getCurrentWatermark();
		if(nextWatermark != null) {
			emitWatermarkIfMarkingProgress(srcContext);
		}
		setNextWatermarkTimer();
	}

	private void setNextWatermarkTimer() {
		long timeToNextWatermark = getTimeToNextWatermark();
		runtime.registerTimer(timeToNextWatermark, this);
	}

	private long getTimeToNextWatermark() {
		return System.currentTimeMillis() + watermarkInterval;
	}
}
