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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.config.OffsetCommitMode;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractFetcher;
import org.apache.flink.streaming.connectors.kafka.internals.AbstractPartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.Kafka08Fetcher;
import org.apache.flink.streaming.connectors.kafka.internals.Kafka08PartitionDiscoverer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicsDescriptor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.flink.util.PropertiesUtil;
import org.apache.flink.util.SerializedValue;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.PropertiesUtil.getBoolean;
import static org.apache.flink.util.PropertiesUtil.getLong;

/**
 * The Flink Kafka Consumer is a streaming data source that pulls a parallel data stream from
 * Apache Kafka 0.8.x. The consumer can run in multiple parallel instances, each of which will pull
 * data from one or more Kafka partitions.
 *
 * <p>The Flink Kafka Consumer participates in checkpointing and guarantees that no data is lost
 * during a failure, and that the computation processes elements "exactly once".
 * (Note: These guarantees naturally assume that Kafka itself does not loose any data.)</p>
 *
 * <p>Flink's Kafka Consumer is designed to be compatible with Kafka's High-Level Consumer API (0.8.x).
 * Most of Kafka's configuration variables can be used with this consumer as well:
 *         <ul>
 *             <li>socket.timeout.ms</li>
 *             <li>socket.receive.buffer.bytes</li>
 *             <li>fetch.message.max.bytes</li>
 *             <li>auto.offset.reset with the values "largest", "smallest"</li>
 *             <li>fetch.wait.max.ms</li>
 *         </ul>
 *
 * <h1>Offset handling</h1>
 *
 * <p>Offsets whose records have been read and are checkpointed will be committed back to ZooKeeper
 * by the offset handler. In addition, the offset handler finds the point where the source initially
 * starts reading from the stream, when the streaming job is started.</p>
 *
 * <p>Please note that Flink snapshots the offsets internally as part of its distributed checkpoints. The offsets
 * committed to Kafka / ZooKeeper are only to bring the outside view of progress in sync with Flink's view
 * of the progress. That way, monitoring and other jobs can get a view of how far the Flink Kafka consumer
 * has consumed a topic.</p>
 *
 * <p>If checkpointing is disabled, the consumer will periodically commit the current offset
 * to Zookeeper.</p>
 *
 * <p>When using a Kafka topic to send data between Flink jobs, we recommend using the
 * {@see TypeInformationSerializationSchema} and {@see TypeInformationKeyValueSerializationSchema}.</p>
 */
@PublicEvolving
public class FlinkKafkaConsumer08<T> extends FlinkKafkaConsumerBase<T> {

	private static final long serialVersionUID = -6272159445203409112L;

	/** Configuration key for the number of retries for getting the partition info. */
	public static final String GET_PARTITIONS_RETRIES_KEY = "flink.get-partitions.retry";

	/** Default number of retries for getting the partition info. One retry means going through the full list of brokers */
	public static final int DEFAULT_GET_PARTITIONS_RETRIES = 3;

	// ------------------------------------------------------------------------

	/** The properties to parametrize the Kafka consumer and ZooKeeper client. */
	private final Properties kafkaProperties;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer08(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
		this(Collections.singletonList(topic), valueDeserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x
	 *
	 * <p>This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from Kafka.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer08(String topic, KeyedDeserializationSchema<T> deserializer, Properties props) {
		this(Collections.singletonList(topic), deserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x
	 *
	 * <p>This constructor allows passing multiple topics to the consumer.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer08(List<String> topics, DeserializationSchema<T> deserializer, Properties props) {
		this(topics, new KeyedDeserializationSchemaWrapper<>(deserializer), props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x
	 *
	 * <p>This constructor allows passing multiple topics and a key/value deserialization schema.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer08(List<String> topics, KeyedDeserializationSchema<T> deserializer, Properties props) {
		this(topics, null, deserializer, props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x. Use this constructor to
	 * subscribe to multiple topics based on a regular expression pattern.
	 *
	 * <p>If partition discovery is enabled (by setting a non-negative value for
	 * {@link FlinkKafkaConsumer08#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the properties), topics
	 * with names matching the pattern will also be subscribed to as they are created on the fly.
	 *
	 * @param subscriptionPattern
	 *           The regular expression for a pattern of topic names to subscribe to.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	@PublicEvolving
	public FlinkKafkaConsumer08(Pattern subscriptionPattern, DeserializationSchema<T> valueDeserializer, Properties props) {
		this(subscriptionPattern, new KeyedDeserializationSchemaWrapper<>(valueDeserializer), props);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.8.x. Use this constructor to
	 * subscribe to multiple topics based on a regular expression pattern.
	 *
	 * <p>If partition discovery is enabled (by setting a non-negative value for
	 * {@link FlinkKafkaConsumer08#KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS} in the properties), topics
	 * with names matching the pattern will also be subscribed to as they are created on the fly.
	 *
	 * <p>This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from Kafka.
	 *
	 * @param subscriptionPattern
	 *           The regular expression for a pattern of topic names to subscribe to.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	@PublicEvolving
	public FlinkKafkaConsumer08(Pattern subscriptionPattern, KeyedDeserializationSchema<T> deserializer, Properties props) {
		this(null, subscriptionPattern, deserializer, props);
	}

	private FlinkKafkaConsumer08(
			List<String> topics,
			Pattern subscriptionPattern,
			KeyedDeserializationSchema<T> deserializer,
			Properties props) {

		super(
				topics,
				subscriptionPattern,
				deserializer,
				getLong(
					checkNotNull(props, "props"),
					KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, PARTITION_DISCOVERY_DISABLED),
				!getBoolean(props, KEY_DISABLE_METRICS, false));

		this.kafkaProperties = props;

		// validate the zookeeper properties
		validateZooKeeperConfig(props);

		// eagerly check for invalid "auto.offset.reset" values before launching the job
		validateAutoOffsetResetValue(props);
	}

	@Override
	protected AbstractFetcher<T, ?> createFetcher(
			SourceContext<T> sourceContext,
			Map<KafkaTopicPartition, Long> assignedPartitionsWithInitialOffsets,
			SerializedValue<AssignerWithPeriodicWatermarks<T>> watermarksPeriodic,
			SerializedValue<AssignerWithPunctuatedWatermarks<T>> watermarksPunctuated,
			StreamingRuntimeContext runtimeContext,
			OffsetCommitMode offsetCommitMode,
			MetricGroup consumerMetricGroup,
			boolean useMetrics) throws Exception {

		long autoCommitInterval = (offsetCommitMode == OffsetCommitMode.KAFKA_PERIODIC)
				? PropertiesUtil.getLong(kafkaProperties, "auto.commit.interval.ms", 60000)
				: -1; // this disables the periodic offset committer thread in the fetcher

		return new Kafka08Fetcher<>(
				sourceContext,
				assignedPartitionsWithInitialOffsets,
				watermarksPeriodic,
				watermarksPunctuated,
				runtimeContext,
				deserializer,
				kafkaProperties,
				autoCommitInterval,
				consumerMetricGroup,
				useMetrics);
	}

	@Override
	protected AbstractPartitionDiscoverer createPartitionDiscoverer(
			KafkaTopicsDescriptor topicsDescriptor,
			int indexOfThisSubtask,
			int numParallelSubtasks) {

		return new Kafka08PartitionDiscoverer(topicsDescriptor, indexOfThisSubtask, numParallelSubtasks, kafkaProperties);
	}

	@Override
	protected boolean getIsAutoCommitEnabled() {
		return PropertiesUtil.getBoolean(kafkaProperties, "auto.commit.enable", true) &&
				PropertiesUtil.getLong(kafkaProperties, "auto.commit.interval.ms", 60000) > 0;
	}

	@Override
	protected Map<KafkaTopicPartition, Long> fetchOffsetsWithTimestamp(Collection<KafkaTopicPartition> partitions, long timestamp) {
		// this should not be reached, since we do not expose the timestamp-based startup feature in version 0.8.
		throw new UnsupportedOperationException(
			"Fetching partition offsets using timestamps is only supported in Kafka versions 0.10 and above.");
	}

	// ------------------------------------------------------------------------
	//  Kafka / ZooKeeper configuration utilities
	// ------------------------------------------------------------------------

	/**
	 * Validate the ZK configuration, checking for required parameters.
	 *
	 * @param props Properties to check
	 */
	protected static void validateZooKeeperConfig(Properties props) {
		if (props.getProperty("zookeeper.connect") == null) {
			throw new IllegalArgumentException("Required property 'zookeeper.connect' has not been set in the properties");
		}
		if (props.getProperty(ConsumerConfig.GROUP_ID_CONFIG) == null) {
			throw new IllegalArgumentException("Required property '" + ConsumerConfig.GROUP_ID_CONFIG
					+ "' has not been set in the properties");
		}

		try {
			//noinspection ResultOfMethodCallIgnored
			Integer.parseInt(props.getProperty("zookeeper.session.timeout.ms", "0"));
		}
		catch (NumberFormatException e) {
			throw new IllegalArgumentException("Property 'zookeeper.session.timeout.ms' is not a valid integer");
		}

		try {
			//noinspection ResultOfMethodCallIgnored
			Integer.parseInt(props.getProperty("zookeeper.connection.timeout.ms", "0"));
		}
		catch (NumberFormatException e) {
			throw new IllegalArgumentException("Property 'zookeeper.connection.timeout.ms' is not a valid integer");
		}
	}

	/**
	 * Check for invalid "auto.offset.reset" values. Should be called in constructor for eager checking before submitting
	 * the job. Note that 'none' is also considered invalid, as we don't want to deliberately throw an exception
	 * right after a task is started.
	 *
	 * @param config kafka consumer properties to check
	 */
	private static void validateAutoOffsetResetValue(Properties config) {
		final String val = config.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "largest");
		if (!(val.equals("largest") || val.equals("latest") || val.equals("earliest") || val.equals("smallest"))) {
			// largest/smallest is kafka 0.8, latest/earliest is kafka 0.9
			throw new IllegalArgumentException("Cannot use '" + ConsumerConfig.AUTO_OFFSET_RESET_CONFIG
				+ "' value '" + val + "'. Possible values: 'latest', 'largest', 'earliest', or 'smallest'.");
		}
	}
}
