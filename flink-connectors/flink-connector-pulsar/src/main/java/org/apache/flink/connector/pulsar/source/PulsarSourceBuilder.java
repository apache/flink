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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.config.ConfigurationDataCustomizer;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.subscriber.PulsarSubscriber;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRange;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicRangeGenerator;
import org.apache.flink.connector.pulsar.source.exception.PulsarRuntimeException;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.utils.PulsarJsonUtils;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_REGEX_SUBSCRIPTION_MODE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_MODE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TOPICS_PATTERN;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TOPIC_NAMES;
import static org.apache.flink.connector.pulsar.source.config.PulsarConfigurationUtils.getOptionValue;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The @builder class for {@link PulsarSource} to make it easier for the users to construct a {@link
 * PulsarSource}.
 *
 * <p>The following example shows the minimum setup to create a PulsarSource that reads the String
 * values from a Pulsar topic.
 *
 * <pre>{@code
 * PulsarSource<byte[], String> source = PulsarSource
 *     .<byte[], String>builder()
 *     .setServiceUrl(MY_BOOTSTRAP_SERVERS)
 *     .setTopics(Arrays.asList(TOPIC1, TOPIC2))
 *     .setSchema(PulsarDeserializationSchema.flinkSchema(Types.STRING))
 *     .build();
 * }</pre>
 *
 * @param <IN> The input type of the pulsar {@link Message <?>}
 * @param <OUT> The output type of the source.
 */
@PublicEvolving
public final class PulsarSourceBuilder<IN, OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarSourceBuilder.class);

    private final Configuration configuration;
    private PulsarSubscriber subscriber;
    private TopicRangeGenerator rangeGenerator;
    private Supplier<StartCursor> startCursorSupplier;
    private Supplier<StopCursor> stopCursorSupplier;
    private Boundedness boundedness;
    private PulsarDeserializationSchema<IN, OUT> deserializationSchema;
    private ConfigurationDataCustomizer<ClientConfigurationData> clientConfigurationCustomizer;
    private ConfigurationDataCustomizer<ConsumerConfigurationData<IN>>
            consumerConfigurationCustomizer;

    // private builder constructor.
    PulsarSourceBuilder() {
        // The default configuration holder.
        this.configuration = new Configuration();
    }

    /**
     * Sets the server's link for the PulsarConsumer of the PulsarSource.
     *
     * @param serviceUrl the server url of the Pulsar cluster.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<IN, OUT> setServiceUrl(String serviceUrl) {
        configuration.set(PULSAR_SERVICE_URL, serviceUrl);
        return this;
    }

    public PulsarSourceBuilder<IN, OUT> setSubscriptionName(String subscriptionName) {
        configuration.set(PULSAR_SUBSCRIPTION_NAME, subscriptionName);
        return this;
    }

    public PulsarSourceBuilder<IN, OUT> setSubscriptionType(SubscriptionType subscriptionType) {
        configuration.set(PULSAR_SUBSCRIPTION_TYPE, subscriptionType);
        return this;
    }

    public PulsarSourceBuilder<IN, OUT> setSubscriptionMode(SubscriptionMode subscriptionMode) {
        configuration.set(PULSAR_SUBSCRIPTION_MODE, subscriptionMode);
        return this;
    }

    public PulsarSourceBuilder<IN, OUT> setTopics(String... topics) {
        return setTopics(Arrays.asList(topics));
    }

    public PulsarSourceBuilder<IN, OUT> setTopics(List<String> topics) {
        ensureSubscriberIsNull("topics");
        configuration.set(PULSAR_TOPIC_NAMES, PulsarJsonUtils.toString(topics));

        this.subscriber = PulsarSubscriber.getTopicListSubscriber(topics);

        return this;
    }

    public PulsarSourceBuilder<IN, OUT> setTopicPattern(
            String topicsPattern, RegexSubscriptionMode regexSubscriptionMode) {
        return setTopicPattern(Pattern.compile(topicsPattern), regexSubscriptionMode);
    }

    public PulsarSourceBuilder<IN, OUT> setTopicPattern(
            Pattern topicsPattern, RegexSubscriptionMode regexSubscriptionMode) {
        ensureSubscriberIsNull("topic pattern");
        configuration.set(PULSAR_TOPICS_PATTERN, topicsPattern.toString());
        configuration.set(PULSAR_REGEX_SUBSCRIPTION_MODE, regexSubscriptionMode);

        this.subscriber =
                PulsarSubscriber.getTopicPatternSubscriber(topicsPattern, regexSubscriptionMode);

        return this;
    }

    public PulsarSourceBuilder<IN, OUT> setRegexSubscriptionMode(
            RegexSubscriptionMode regexSubscriptionMode) {
        configuration.set(PULSAR_REGEX_SUBSCRIPTION_MODE, regexSubscriptionMode);
        return this;
    }

    /**
     * Set a topic range generator for different topics.
     *
     * @param rangeGenerator A generator which would generate a set of {@link TopicRange} for given
     *     topic.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<IN, OUT> setRangeGenerator(TopicRangeGenerator rangeGenerator) {
        this.rangeGenerator = rangeGenerator;
        return this;
    }

    /**
     * Specify from which offsets the PulsarSource should start consume from by providing an {@link
     * StartCursor}.
     *
     * @param startCursorSupplier the supplier providing a {@link StartCursor} which set the
     *     starting offsets for the Source.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<IN, OUT> setStartCursorSupplier(
            Supplier<StartCursor> startCursorSupplier) {
        this.startCursorSupplier = startCursorSupplier;
        return this;
    }

    /**
     * By default the PulsarSource is set to run in {@link Boundedness#CONTINUOUS_UNBOUNDED} manner
     * and thus never stops until the Flink job fails or is canceled. To let the PulsarSource run as
     * a streaming source but still stops at some point, one can set an {@link StartCursor} to
     * specify the stopping offsets for each partition. When all the partitions have reached their
     * stopping offsets, the PulsarSource will then exit.
     *
     * <p>This method is different from {@link #setBounded(Supplier)} that after setting the
     * stopping offsets with this method, {@link PulsarSource#getBoundedness()} will still return
     * {@link Boundedness#CONTINUOUS_UNBOUNDED} even though it will stop at the stopping offsets
     * specified by the stopping offsets {@link StartCursor}.
     *
     * @param stopCursorSupplier The {@link StopCursor} to specify the stopping offset.
     * @return this PulsarSourceBuilder.
     * @see #setBounded(Supplier)
     */
    public PulsarSourceBuilder<IN, OUT> setUnbounded(Supplier<StopCursor> stopCursorSupplier) {
        this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        this.stopCursorSupplier = stopCursorSupplier;
        return this;
    }

    /**
     * By default the PulsarSource is set to run in {@link Boundedness#CONTINUOUS_UNBOUNDED} manner
     * and thus never stops until the Flink job fails or is canceled. To let the PulsarSource run in
     * {@link Boundedness#BOUNDED} manner and stops at some point, one can set an {@link
     * StartCursor} to specify the stopping offsets for each partition. When all the partitions have
     * reached their stopping offsets, the PulsarSource will then exit.
     *
     * <p>This method is different from {@link #setUnbounded(Supplier)} that after setting the
     * stopping offsets with this method, {@link PulsarSource#getBoundedness()} will return {@link
     * Boundedness#BOUNDED} instead of {@link Boundedness#CONTINUOUS_UNBOUNDED}.
     *
     * @param stopCursorSupplier the {@link StopCursor} to specify the stopping offsets.
     * @return this PulsarSourceBuilder.
     * @see #setUnbounded(Supplier)
     */
    public PulsarSourceBuilder<IN, OUT> setBounded(Supplier<StopCursor> stopCursorSupplier) {
        this.boundedness = Boundedness.BOUNDED;
        this.stopCursorSupplier = stopCursorSupplier;
        return this;
    }

    public PulsarSourceBuilder<IN, OUT> setDeserializationSchema(
            PulsarDeserializationSchema<IN, OUT> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
        return this;
    }

    public PulsarSourceBuilder<IN, OUT> modifyClientConfig(
            Consumer<ClientConfigurationData> consumer) {
        if (clientConfigurationCustomizer == null) {
            this.clientConfigurationCustomizer = consumer::accept;
        } else {
            this.clientConfigurationCustomizer =
                    clientConfigurationCustomizer.compose(consumer::accept);
        }
        return this;
    }

    public PulsarSourceBuilder<IN, OUT> modifyConsumerConfig(
            Consumer<ConsumerConfigurationData<IN>> consumer) {
        if (consumerConfigurationCustomizer == null) {
            this.consumerConfigurationCustomizer = consumer::accept;
        } else {
            this.consumerConfigurationCustomizer =
                    consumerConfigurationCustomizer.compose(consumer::accept);
        }
        return this;
    }

    /**
     * Set an arbitrary property for the PulsarSource and PulsarConsumer. The valid keys can be
     * found in {@link PulsarSourceOptions}.
     *
     * @param key the key of the property.
     * @param value the value of the property.
     * @return this PulsarSourceBuilder.
     */
    public PulsarSourceBuilder<IN, OUT> setProperty(String key, String value) {
        configuration.setString(key, value);
        return this;
    }

    public <T> PulsarSourceBuilder<IN, OUT> setProperty(ConfigOption<T> key, T value) {
        configuration.set(key, value);
        return this;
    }

    public PulsarSourceBuilder<IN, OUT> setProperties(Properties properties) {
        // Convert all the Properties value into String.
        properties
                .stringPropertyNames()
                .forEach(
                        name -> {
                            Object value = properties.get(name);
                            if (value == null) {
                                // Could fallback to default Properties value.
                                value = properties.getProperty(name);
                            }

                            if (value != null) {
                                if (configuration.containsKey(name)) {
                                    ConfigOption<String> rawOption =
                                            ConfigOptions.key(name).stringType().noDefaultValue();
                                    LOG.warn(
                                            "Config option {} already has a value {}, override to new value {}",
                                            name,
                                            configuration.getString(rawOption),
                                            value);
                                }
                                configuration.setString(name, Objects.toString(value));
                            }
                        });
        return this;
    }

    /**
     * Build the {@link PulsarSource}.
     *
     * @return a PulsarSource with the settings made for this builder.
     */
    public PulsarSource<IN, OUT> build() {
        // Check builder configuration.

        // Ensure the topics for pulsar.
        if (subscriber == null) {
            if (configuration.contains(PULSAR_TOPIC_NAMES)) {
                List<String> topics =
                        getOptionValue(
                                configuration,
                                PULSAR_TOPIC_NAMES,
                                names -> PulsarJsonUtils.toList(String.class, names));
                this.subscriber = PulsarSubscriber.getTopicListSubscriber(topics);
            } else if (configuration.contains(PULSAR_TOPICS_PATTERN)) {
                String topicPatternStr = configuration.get(PULSAR_TOPICS_PATTERN);
                Pattern topicPattern = Pattern.compile(topicPatternStr);
                RegexSubscriptionMode regexSubscriptionMode =
                        configuration.get(PULSAR_REGEX_SUBSCRIPTION_MODE);

                this.subscriber =
                        PulsarSubscriber.getTopicPatternSubscriber(
                                topicPattern, regexSubscriptionMode);
            } else {
                throw new PulsarRuntimeException(
                        "No "
                                + PULSAR_TOPIC_NAMES.key()
                                + " or "
                                + PULSAR_TOPICS_PATTERN.key()
                                + " was provided.");
            }
        }
        if (rangeGenerator == null) {
            this.rangeGenerator = TopicRangeGenerator.DEFAULT_RANGE_GENERATOR;
        }
        if (startCursorSupplier == null) {
            this.startCursorSupplier = StartCursor::defaultStartCursor;
        }
        if (stopCursorSupplier == null) {
            this.stopCursorSupplier = StopCursor::defaultStopCursor;
        }
        if (boundedness == null) {
            this.boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        }
        if (boundedness == Boundedness.BOUNDED
                && configuration.get(PARTITION_DISCOVERY_INTERVAL_MS) >= 0) {
            LOG.warn(
                    "{} property is overridden to -1 because the source is bounded.",
                    PARTITION_DISCOVERY_INTERVAL_MS);
            configuration.set(PARTITION_DISCOVERY_INTERVAL_MS, -1L);
        }
        checkNotNull(deserializationSchema, "deserializationSchema should be set.");
        if (clientConfigurationCustomizer == null) {
            this.clientConfigurationCustomizer = ConfigurationDataCustomizer.blankCustomizer();
        }
        if (consumerConfigurationCustomizer == null) {
            this.consumerConfigurationCustomizer = ConfigurationDataCustomizer.blankCustomizer();
        }

        return new PulsarSource<>(
                configuration,
                subscriber,
                rangeGenerator,
                startCursorSupplier,
                stopCursorSupplier,
                boundedness,
                deserializationSchema,
                clientConfigurationCustomizer,
                consumerConfigurationCustomizer);
    }

    // ------------- private helpers  --------------

    private void ensureSubscriberIsNull(String attemptingSubscribeMode) {
        if (subscriber != null) {
            throw new IllegalStateException(
                    String.format(
                            "Cannot use %s for consumption because a %s is already set for consumption.",
                            attemptingSubscribeMode, subscriber.getClass().getSimpleName()));
        }
    }
}
