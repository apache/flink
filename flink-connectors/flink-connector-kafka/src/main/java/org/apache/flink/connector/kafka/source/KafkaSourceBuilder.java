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

package org.apache.flink.connector.kafka.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.enumerator.initializer.NoStoppingOffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The @builder class for {@link KafkaSource} to make it easier for the users to construct
 * a {@link KafkaSource}.
 *
 * <p>The following example shows the minimum setup to create a KafkaSource that reads the
 * String values from a Kafka topic.
 * <pre>{@code
 * KafkaSource<String> source = KafkaSource
 *     .<String>builder()
 *     .setBootstrapServers(MY_BOOTSTRAP_SERVERS)
 *     .setGroupId("myGroup")
 *     .setTopics(Arrays.asList(TOPIC1, TOPIC2))
 *     .setDeserializer(KafkaRecordDeserializer.valueOnly(StringDeserializer.class))
 *     .build();
 * }</pre>
 * The bootstrap servers, group id, topics/partitions to consume, and the record deserializer
 * are required fields that must be set.
 *
 * <p>To specify the starting offsets of the KafkaSource, one can call
 * {@link #setStartingOffsets(OffsetsInitializer)}.
 *
 * <p>By default the KafkaSource runs in an {@link Boundedness#CONTINUOUS_UNBOUNDED} mode
 * and never stops until the Flink job is canceled or fails. To let the KafkaSource run
 * in {@link Boundedness#CONTINUOUS_UNBOUNDED} but stops at some given offsets, one can
 * call {@link #setUnbounded(OffsetsInitializer)}. For example the following KafkaSource
 * stops after it consumes up to the latest partition offsets at the point when the Flink
 * started.
 * <pre>{@code
 * KafkaSource<String> source = KafkaSource
 *     .<String>builder()
 *     .setBootstrapServers(MY_BOOTSTRAP_SERVERS)
 *     .setGroupId("myGroup")
 *     .setTopics(Arrays.asList(TOPIC1, TOPIC2))
 *     .setDeserializer(KafkaRecordDeserializer.valueOnly(StringDeserializer.class))
 *     .setUnbounded(OffsetsInitializer.latest())
 *     .build();
 * }</pre>
 *
 * <p>Check the Java docs of each individual methods to learn more about the settings to
 * build a KafkaSource.
 */
public class KafkaSourceBuilder<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaSourceBuilder.class);
	private static final String[] REQUIRED_CONFIGS = {
		ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
		ConsumerConfig.GROUP_ID_CONFIG};
	// The subscriber specifies the partitions to subscribe to.
	private KafkaSubscriber subscriber;
	// Users can specify the starting / stopping offset initializer.
	private OffsetsInitializer startingOffsetsInitializer;
	private OffsetsInitializer stoppingOffsetsInitializer;
	// Boundedness
	private Boundedness boundedness;
	private KafkaRecordDeserializer<OUT> deserializationSchema;
	// The configurations.
	protected Properties props;

	KafkaSourceBuilder() {
		this.subscriber = null;
		this.startingOffsetsInitializer = OffsetsInitializer.earliest();
		this.stoppingOffsetsInitializer = new NoStoppingOffsetsInitializer();
		this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
		this.deserializationSchema = null;
		this.props = new Properties();
	}

	/**
	 * Sets the bootstrap servers for the KafkaConsumer of the KafkaSource.
	 *
	 * @param bootstrapServers the bootstrap servers of the Kafka cluster.
	 * @return this KafkaSourceBuilder.
	 */
	public KafkaSourceBuilder<OUT> setBootstrapServers(String bootstrapServers) {
		return setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
	}

	/**
	 * Sets the consumer group id of the KafkaSource.
	 *
	 * @param groupId the group id of the KafkaSource.
	 * @return this KafkaSourceBuilder.
	 */
	public KafkaSourceBuilder<OUT> setGroupId(String groupId) {
		return setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	}

	/**
	 * Set a list of topics the KafkaSource should consume from. All the topics
	 * in the list should have existed in the Kafka cluster. Otherwise an
	 * exception will be thrown. To allow some of the topics to be created lazily,
	 * please use {@link #setTopicPattern(Pattern)} instead.
	 *
	 * @param topics the list of topics to consume from.
	 * @return this KafkaSourceBuilder.
	 * @see org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Collection)
	 */
	public KafkaSourceBuilder<OUT> setTopics(List<String> topics) {
		ensureSubscriberIsNull("topics");
		subscriber = KafkaSubscriber.getTopicListSubscriber(topics);
		return this;
	}

	/**
	 * Set a list of topics the KafkaSource should consume from. All the topics
	 * in the list should have existed in the Kafka cluster. Otherwise an
	 * exception will be thrown. To allow some of the topics to be created lazily,
	 * please use {@link #setTopicPattern(Pattern)} instead.
	 *
	 * @param topics the list of topics to consume from.
	 * @return this KafkaSourceBuilder.
	 * @see org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Collection)
	 */
	public KafkaSourceBuilder<OUT> setTopics(String... topics) {
		return setTopics(Arrays.asList(topics));
	}

	/**
	 * Set a topic pattern to consume from use the java {@link Pattern}.
	 *
	 * @param topicPattern the pattern of the topic name to consume from.
	 * @return this KafkaSourceBuilder.
	 * @see org.apache.kafka.clients.consumer.KafkaConsumer#subscribe(Pattern)
	 */
	public KafkaSourceBuilder<OUT> setTopicPattern(Pattern topicPattern) {
		ensureSubscriberIsNull("topic pattern");
		subscriber = KafkaSubscriber.getTopicPatternSubscriber(topicPattern);
		return this;
	}

	/**
	 * Set a set of partitions to consume from.
	 *
	 * @param partitions the set of partitions to consume from.
	 * @return this KafkaSourceBuilder.
	 * @see org.apache.kafka.clients.consumer.KafkaConsumer#assign(Collection)
	 */
	public KafkaSourceBuilder<OUT> setPartitions(Set<TopicPartition> partitions) {
		ensureSubscriberIsNull("partitions");
		subscriber = KafkaSubscriber.getPartitionSetSubscriber(partitions);
		return this;
	}

	/**
	 * Specify from which offsets the KafkaSource should start consume from by providing
	 * an {@link OffsetsInitializer}.
	 *
	 * <p>The following {@link OffsetsInitializer}s are commonly used and provided out of the box.
	 * Users can also implement their own {@link OffsetsInitializer} for custom behaviors.
	 * <ul>
	 *     <li>{@link OffsetsInitializer#earliest()} - starting from the earliest offsets. This is also the default
	 *         {@link OffsetsInitializer} of the KafkaSource for starting offsets.
	 *     </li>
	 *     <li>
	 *         {@link OffsetsInitializer#latest()} - starting from the latest offsets.
	 *     </li>
	 *     <li>
	 *         {@link OffsetsInitializer#committedOffsets()} - starting from the committed offsets of the
	 *         consumer group.
	 *     </li>
	 *     <li>
	 *         {@link OffsetsInitializer#committedOffsets(org.apache.kafka.clients.consumer.OffsetResetStrategy)}
	 *         - starting from the committed offsets of the consumer group. If there is no committed offsets,
	 *         starting from the offsets specified by the
	 *         {@link org.apache.kafka.clients.consumer.OffsetResetStrategy OffsetResetStrategy}.
	 *     </li>
	 *     <li>
	 *         {@link OffsetsInitializer#offsets(Map)} - starting from the specified offsets for each partition.
	 *     </li>
	 *     <li>
	 *         {@link OffsetsInitializer#timestamp(long)} - starting from the specified timestamp for each partition.
	 *         Note that the guarantee here is that all the records in Kafka whose
	 *         {@link org.apache.kafka.clients.consumer.ConsumerRecord#timestamp()} is greater than the given
	 *         starting timestamp will be consumed. However, it is possible that some consumer records
	 *         whose timestamp is smaller than the given starting timestamp are also consumed.
	 *     </li>
	 * </ul>
	 *
	 * @param startingOffsetsInitializer the {@link OffsetsInitializer} setting the starting offsets for the Source.
	 * @return this KafkaSourceBuilder.
	 */
	public KafkaSourceBuilder<OUT> setStartingOffsets(OffsetsInitializer startingOffsetsInitializer) {
		this.startingOffsetsInitializer = startingOffsetsInitializer;
		return this;
	}

	/**
	 * By default the KafkaSource is set to run in {@link Boundedness#CONTINUOUS_UNBOUNDED} manner
	 * and thus never stops until the Flink job fails or is canceled. To let the KafkaSource run as
	 * a streaming source but still stops at some point, one can set an {@link OffsetsInitializer}
	 * to specify the stopping offsets for each partition. When all the partitions have reached
	 * their stopping offsets, the KafkaSource will then exit.
	 *
	 * <p>This method is different from {@link #setBounded(OffsetsInitializer)} that after
	 * setting the stopping offsets with this method, {@link KafkaSource#getBoundedness()}
	 * will still return {@link Boundedness#CONTINUOUS_UNBOUNDED} even though it will stop at
	 * the stopping offsets specified by the stopping offsets {@link OffsetsInitializer}.
	 *
	 * <p>The following {@link OffsetsInitializer} are commonly used and provided out of the box.
	 * Users can also implement their own {@link OffsetsInitializer} for custom behaviors.
	 * <ul>
	 *     <li>
	 *         {@link OffsetsInitializer#latest()} - stop at the latest offsets of the partitions
	 *         when the KafkaSource starts to run.
	 *     </li>
	 *     <li>
	 *         {@link OffsetsInitializer#committedOffsets()} - stops at the committed offsets of the consumer group.
	 *     </li>
	 *     <li>
	 *         {@link OffsetsInitializer#offsets(Map)} - stops at the specified offsets for each partition.
	 *     </li>
	 *     <li>
	 *         {@link OffsetsInitializer#timestamp(long)} - stops at the specified timestamp for
	 *         each partition. The guarantee of setting the stopping timestamp is that no Kafka records
	 *         whose {@link org.apache.kafka.clients.consumer.ConsumerRecord#timestamp()} is greater
	 *         than the given stopping timestamp will be consumed. However, it is possible that
	 *         some records whose timestamp is smaller than the specified stopping timestamp are
	 *         not consumed.
	 *     </li>
	 * </ul>
	 *
	 * @param stoppingOffsetsInitializer The {@link OffsetsInitializer} to specify the stopping offset.
	 * @return this KafkaSourceBuilder.
	 * @see #setBounded(OffsetsInitializer)
	 */
	public KafkaSourceBuilder<OUT> setUnbounded(OffsetsInitializer stoppingOffsetsInitializer) {
		this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
		this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
		return this;
	}

	/**
	 * By default the KafkaSource is set to run in {@link Boundedness#CONTINUOUS_UNBOUNDED} manner
	 * and thus never stops until the Flink job fails or is canceled. To let the KafkaSource run in
	 * {@link Boundedness#BOUNDED} manner and stops at some point, one can set an
	 * {@link OffsetsInitializer} to specify the stopping offsets for each partition. When all the
	 * partitions have reached their stopping offsets, the KafkaSource will then exit.
	 *
	 * <p>This method is different from {@link #setUnbounded(OffsetsInitializer)} that after
	 * setting the stopping offsets with this method, {@link KafkaSource#getBoundedness()}
	 * will return {@link Boundedness#BOUNDED} instead of {@link Boundedness#CONTINUOUS_UNBOUNDED}.
	 *
	 * <p>The following {@link OffsetsInitializer} are commonly used and provided out of the box.
	 * Users can also implement their own {@link OffsetsInitializer} for custom behaviors.
	 * <ul>
	 *     <li>
	 *         {@link OffsetsInitializer#latest()} - stop at the latest offsets of the partitions
	 *         when the KafkaSource starts to run.
	 *     </li>
	 *     <li>
	 *         {@link OffsetsInitializer#committedOffsets()} - stops at the committed offsets
	 *         of the consumer group.
	 *     </li>
	 *     <li>
	 *         {@link OffsetsInitializer#offsets(Map)} - stops at the specified offsets for each partition.
	 *     </li>
	 *     <li>
	 *         {@link OffsetsInitializer#timestamp(long)} - stops at the specified timestamp for
	 *         each partition. The guarantee of setting the stopping timestamp is that no Kafka records
	 *         whose {@link org.apache.kafka.clients.consumer.ConsumerRecord#timestamp()} is greater
	 *         than the given stopping timestamp will be consumed. However, it is possible that
	 *         some records whose timestamp is smaller than the specified stopping timestamp are
	 *         not consumed.
	 *     </li>
	 * </ul>
	 *
	 * @param stoppingOffsetsInitializer the {@link OffsetsInitializer} to specify the stopping offsets.
	 * @return this KafkaSourceBuilder.
	 * @see #setUnbounded(OffsetsInitializer)
	 */
	public KafkaSourceBuilder<OUT> setBounded(OffsetsInitializer stoppingOffsetsInitializer) {
		this.boundedness = Boundedness.BOUNDED;
		this.stoppingOffsetsInitializer = stoppingOffsetsInitializer;
		return this;
	}

	/**
	 * Sets the {@link KafkaRecordDeserializer deserializer} of the
	 * {@link org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord} for KafkaSource.
	 *
	 * @param recordDeserializer the deserializer for Kafka
	 *                           {@link org.apache.kafka.clients.consumer.ConsumerRecord ConsumerRecord}.
	 * @return this KafkaSourceBuilder.
	 */
	public KafkaSourceBuilder<OUT> setDeserializer(KafkaRecordDeserializer<OUT> recordDeserializer) {
		this.deserializationSchema = recordDeserializer;
		return this;
	}

	/**
	 * Sets the client id prefix of this KafkaSource.
	 *
	 * @param prefix the client id prefix to use for this KafkaSource.
	 * @return this KafkaSourceBuilder.
	 */
	public KafkaSourceBuilder<OUT> setClientIdPrefix(String prefix) {
		return setProperty(KafkaSourceOptions.CLIENT_ID_PREFIX.key(), prefix);
	}

	/**
	 * Set an arbitrary property for the KafkaSource and KafkaConsumer. The valid keys can be found
	 * in {@link ConsumerConfig} and {@link KafkaSourceOptions}.
	 *
	 * <p>Note that the following keys will be overridden by the builder when the KafkaSource is
	 * created.
	 * <ul>
	 *    <li>
	 *        <code>key.deserializer</code> is always set to {@link ByteArrayDeserializer}.
	 *    </li>
	 *    <li>
	 *        <code>value.deserializer</code> is always set to {@link ByteArrayDeserializer}.
	 *    </li>
	 *    <li>
	 *        <code>auto.offset.reset.strategy</code> is overridden by {@link OffsetsInitializer#getAutoOffsetResetStrategy()}
	 *        for the starting offsets, which is by default {@link OffsetsInitializer#earliest()}.
	 *    </li>
	 *    <li>
	 *        <code>partition.discovery.interval.ms</code> is overridden to -1 when
	 *        {@link #setBounded(OffsetsInitializer)} has been invoked.
	 *    </li>
	 * </ul>
	 *
	 * @param key the key of the property.
	 * @param value the value of the property.
	 * @return this KafkaSourceBuilder.
	 */
	public KafkaSourceBuilder<OUT> setProperty(String key, String value) {
		props.setProperty(key, value);
		return this;
	}

	/**
	 * Set arbitrary properties for the KafkaSource and KafkaConsumer. The valid keys can be found
	 * in {@link ConsumerConfig} and {@link KafkaSourceOptions}.
	 *
	 * <p>Note that the following keys will be overridden by the builder when the KafkaSource is
	 * created.
	 * <ul>
	 *    <li>
	 *        <code>key.deserializer</code> is always set to {@link ByteArrayDeserializer}.
	 *    </li>
	 *    <li>
	 *        <code>value.deserializer</code> is always set to {@link ByteArrayDeserializer}.
	 *    </li>
	 *    <li>
	 *        <code>auto.offset.reset.strategy</code> is overridden by {@link OffsetsInitializer#getAutoOffsetResetStrategy()}
	 *        for the starting offsets, which is by default {@link OffsetsInitializer#earliest()}.
	 *    </li>
	 *    <li>
	 *        <code>partition.discovery.interval.ms</code> is overridden to -1 when
	 *        {@link #setBounded(OffsetsInitializer)} has been invoked.
	 *    </li>
	 *    <li>
	 *        <code>client.id</code> is overridden to the "client.id.prefix-RANDOM_LONG", or "group.id-RANDOM_LONG" if
	 *        the client id prefix is not set.
	 *    </li>
	 * </ul>
	 *
	 * @param props the properties to set for the KafkaSource.
	 * @return this KafkaSourceBuilder.
	 */
	public KafkaSourceBuilder<OUT> setProperties(Properties props) {
		this.props.putAll(props);
		return this;
	}

	/**
	 * Build the {@link KafkaSource}.
	 *
	 * @return a KafkaSource with the settings made for this builder.
	 */
	public KafkaSource<OUT> build() {
		sanityCheck();
		parseAndSetRequiredProperties();
		return new KafkaSource<>(
				subscriber,
				startingOffsetsInitializer,
				stoppingOffsetsInitializer,
				boundedness,
				deserializationSchema,
				props);
	}

	// ------------- private helpers  --------------

	private void ensureSubscriberIsNull(String attemptingSubscribeMode) {
		if (subscriber != null) {
			throw new IllegalStateException(String.format(
					"Cannot use %s for consumption because a %s is already set for consumption.",
					attemptingSubscribeMode, subscriber.getClass().getSimpleName()));
		}
	}

	private void parseAndSetRequiredProperties() {
		maybeOverride(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(), true);
		maybeOverride(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName(), true);
		maybeOverride(ConsumerConfig.GROUP_ID_CONFIG, "KafkaSource-" + new Random().nextLong(), false);
		maybeOverride(
				ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				startingOffsetsInitializer.getAutoOffsetResetStrategy().name().toLowerCase(),
				true);

		// If the source is bounded, do not run periodic partition discovery.
		if (maybeOverride(
				KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS.key(),
				"-1",
				boundedness == Boundedness.BOUNDED)) {
			LOG.warn("{} property is overridden to -1 because the source is bounded.",
					KafkaSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS);
		}

		// If the client id prefix is not set, reuse the consumer group id as the client id prefix.
		maybeOverride(
				KafkaSourceOptions.CLIENT_ID_PREFIX.key(),
				props.getProperty(ConsumerConfig.GROUP_ID_CONFIG),
				false);
	}

	private boolean maybeOverride(String key, String value, boolean override) {
		boolean overridden = false;
		String userValue = props.getProperty(key);
		if (userValue != null) {
			if (override) {
				LOG.warn(String.format("Property %s is provided but will be overridden from %s to %s",
						key, userValue, value));
				props.setProperty(key, value);
				overridden = true;
			}
		} else {
			props.setProperty(key, value);
		}
		return overridden;
	}

	private void sanityCheck() {
		// Check required configs.
		for (String requiredConfig : REQUIRED_CONFIGS) {
			checkNotNull(props.getProperty(
					requiredConfig,
					String.format("Property %s is required but not provided", requiredConfig)));
		}
		// Check required settings.
		checkNotNull(subscriber, "No subscribe mode is specified, " +
				"should be one of topics, topic pattern and partition set.");
		checkNotNull(deserializationSchema, "Deserialization schema is required but not provided.");
	}
}
