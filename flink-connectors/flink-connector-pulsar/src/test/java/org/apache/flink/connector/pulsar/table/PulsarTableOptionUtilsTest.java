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

package org.apache.flink.connector.pulsar.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.router.TopicRoutingMode;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StopCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.start.MessageIdStartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.start.TimestampStartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.stop.PublishTimestampStopCursor;
import org.apache.flink.connector.pulsar.table.testutils.MockTopicRouter;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.MessageIdImpl;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_STATS_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.createKeyFormatProjection;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.createValueFormatProjection;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getMessageDelayMillis;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getPulsarProperties;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getStartCursor;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getStopCursor;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getSubscriptionType;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getTopicListFromOptions;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getTopicRouter;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.getTopicRoutingMode;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.parseAfterMessageIdStopCursor;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.parseAtMessageIdStopCursor;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.parseMessageIdStartCursor;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptionUtils.parseMessageIdString;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_CUSTOM_TOPIC_ROUTER;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_MESSAGE_DELAY_INTERVAL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_TOPIC_ROUTING_MODE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_STOP_AT_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.TOPICS;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Unit test for {@link PulsarTableOptionUtils}. Tests each method and different inputs. Some tests
 * have overlapping semantics with {@link PulsarTableOptionsTest} and {@link
 * PulsarTableValidationUtilsTest}, but they cover different aspects of the validation, so all of
 * them should be kept.
 */
public class PulsarTableOptionUtilsTest {
    // --------------------------------------------------------------------------------------------
    // Format and Projection  Test
    // --------------------------------------------------------------------------------------------
    @Test
    void formatProjection() {
        final DataType dataType =
                DataTypes.ROW(
                        FIELD("id", INT()),
                        FIELD("name", STRING()),
                        FIELD("age", INT()),
                        FIELD("address", STRING()));

        final Map<String, String> options = createTestOptions();
        options.put("key.fields", "address; name");

        final Configuration config = Configuration.fromMap(options);

        assertThat(createKeyFormatProjection(config, dataType)).containsExactly(3, 1);
        assertThat(createValueFormatProjection(config, dataType)).containsExactly(0, 2);
    }

    @Test
    void invalidKeyFormatFieldProjection() {
        final DataType dataType = ROW(FIELD("id", INT()), FIELD("name", STRING()));
        final Map<String, String> options = createTestOptions();
        options.put("key.fields", "non_existing");

        final Configuration config = Configuration.fromMap(options);

        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> createKeyFormatProjection(config, dataType))
                .withMessage(
                        String.format(
                                "Could not find the field '%s' in the table schema for usage in the key format. "
                                        + "A key field must be a regular, physical column. "
                                        + "The following columns can be selected in the '%s' option: [id, name]",
                                "non_existing", KEY_FIELDS.key()));
    }

    static Map<String, String> createTestOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("key.format", "test-format");
        options.put("key.test-format.delimiter", ",");
        options.put("value.format", "test-format");
        options.put("value.test-format.delimiter", "|");
        options.put("value.test-format.fail-on-missing", "true");
        return options;
    }

    // --------------------------------------------------------------------------------------------
    // Table Source Option Utils Test
    // --------------------------------------------------------------------------------------------

    @Test
    void topicsList() {
        final Map<String, String> options = createDefaultOptions();
        options.put(TOPICS.key(), ";");
        List<String> topicList = getTopicListFromOptions(Configuration.fromMap(options));
        assertThat(topicList).isEmpty();

        options.put(TOPICS.key(), "topic1;");
        topicList = getTopicListFromOptions(Configuration.fromMap(options));
        assertThat(topicList).hasSize(1);

        options.put(TOPICS.key(), "topic1;topic2");
        topicList = getTopicListFromOptions(Configuration.fromMap(options));
        assertThat(topicList).hasSize(2);

        options.put(TOPICS.key(), "");
        topicList = getTopicListFromOptions(Configuration.fromMap(options));
        assertThat(topicList).isEmpty();
    }

    @Test
    void pulsarProperties() {
        final Map<String, String> options = createDefaultOptions();
        options.put(PULSAR_STATS_INTERVAL_SECONDS.key(), "30");
        Properties properties = getPulsarProperties(Configuration.fromMap(options));
        assertThat(properties.getProperty(PULSAR_STATS_INTERVAL_SECONDS.key())).isEqualTo("30");
    }

    @Test
    void startCursor() {
        // TDOO Use isEqualTo() to assert; need equals() method
        final Map<String, String> options = createDefaultOptions();
        options.put(SOURCE_START_FROM_MESSAGE_ID.key(), "earliest");
        StartCursor startCursor = getStartCursor(Configuration.fromMap(options));
        assertThat(startCursor).isInstanceOf(MessageIdStartCursor.class);

        options.put(SOURCE_START_FROM_MESSAGE_ID.key(), "latest");
        startCursor = getStartCursor(Configuration.fromMap(options));
        assertThat(startCursor).isInstanceOf(MessageIdStartCursor.class);

        options.put(SOURCE_START_FROM_MESSAGE_ID.key(), "0:0:-1");
        startCursor = getStartCursor(Configuration.fromMap(options));
        assertThat(startCursor).isInstanceOf(MessageIdStartCursor.class);

        options.put(SOURCE_START_FROM_MESSAGE_ID.key(), "other");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getStartCursor(Configuration.fromMap(options)))
                .withMessage("MessageId format must be ledgerId:entryId:partitionId.");

        options.remove(SOURCE_START_FROM_MESSAGE_ID.key());
        options.put(SOURCE_START_FROM_PUBLISH_TIME.key(), "123545");
        startCursor = getStartCursor(Configuration.fromMap(options));
        assertThat(startCursor).isInstanceOf(TimestampStartCursor.class);

        options.put(SOURCE_START_FROM_PUBLISH_TIME.key(), "123545L");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getStartCursor(Configuration.fromMap(options)))
                .withMessage(
                        "Could not parse value '123545L' for key 'source.start.publish-time'.");
    }

    @Test
    void subscriptionType() {
        final Map<String, String> options = createDefaultOptions();
        options.put(SOURCE_SUBSCRIPTION_TYPE.key(), "Shared");
        SubscriptionType subscriptionType = getSubscriptionType(Configuration.fromMap(options));
        assertThat(subscriptionType).isEqualTo(SubscriptionType.Shared);

        options.put(SOURCE_SUBSCRIPTION_TYPE.key(), "Exclusive");
        subscriptionType = getSubscriptionType(Configuration.fromMap(options));
        assertThat(subscriptionType).isEqualTo(SubscriptionType.Exclusive);
    }

    @Test
    void canParseMessageIdEarliestOrLatestStartCursor() {
        String earliest = "earliest";
        StartCursor startCursor = parseMessageIdStartCursor(earliest);
        assertThat(startCursor).isEqualTo(StartCursor.earliest());

        String latest = "latest";
        startCursor = parseMessageIdStartCursor(latest);
        assertThat(startCursor).isEqualTo(StartCursor.latest());

        String precise = "0:0:100";
        startCursor = parseMessageIdStartCursor(precise);
        assertThat(startCursor).isEqualTo(StartCursor.fromMessageId(new MessageIdImpl(0, 0, 100)));
    }

    @Test
    void publishTimeStartCursor() {
        final Map<String, String> options = createDefaultOptions();
        options.put(SOURCE_START_FROM_PUBLISH_TIME.key(), "12345");
        StartCursor startCursor = getStartCursor(Configuration.fromMap(options));
        assertThat(startCursor).isInstanceOf(TimestampStartCursor.class);

        options.put(SOURCE_START_FROM_PUBLISH_TIME.key(), "12345L");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getStartCursor(Configuration.fromMap(options)))
                .withMessage("Could not parse value '12345L' for key 'source.start.publish-time'.");
    }

    @Test
    void canParseMessageIdNeverOrLatestStopCursor() {
        String never = "never";
        StopCursor stopCursor = parseAtMessageIdStopCursor(never);
        assertThat(stopCursor).isEqualTo(StopCursor.never());

        String latest = "latest";
        stopCursor = parseAtMessageIdStopCursor(latest);
        assertThat(stopCursor).isEqualTo(StopCursor.latest());

        String precise = "0:0:100";
        stopCursor = parseAtMessageIdStopCursor(precise);
        assertThat(stopCursor).isEqualTo(StopCursor.atMessageId(new MessageIdImpl(0, 0, 100)));

        stopCursor = parseAfterMessageIdStopCursor(precise);
        assertThat(stopCursor).isEqualTo(StopCursor.afterMessageId(new MessageIdImpl(0, 0, 100)));
    }

    @Test
    void publishTimeStopCursor() {
        final Map<String, String> options = createDefaultOptions();
        options.put(SOURCE_STOP_AT_PUBLISH_TIME.key(), "12345");
        StopCursor stopCursor = getStopCursor(Configuration.fromMap(options));
        assertThat(stopCursor).isInstanceOf(PublishTimestampStopCursor.class);

        options.put(SOURCE_STOP_AT_PUBLISH_TIME.key(), "12345L");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getStopCursor(Configuration.fromMap(options)))
                .withMessage(
                        "Could not parse value '12345L' for key 'source.stop.at-publish-time'.");
    }

    @Test
    void canParseMessageIdUsingMessageIdImpl() {
        final String invalidFormatMessage =
                "MessageId format must be ledgerId:entryId:partitionId.";
        final String invalidNumberMessage =
                "MessageId format must be ledgerId:entryId:partitionId. Each id should be able to parsed to long type.";
        String precise = "0:0:100";
        assertThatNoException().isThrownBy(() -> parseMessageIdString(precise));

        String empty = "";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseMessageIdString(empty))
                .withMessage(invalidFormatMessage);

        String noSemicolon = "0";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseMessageIdString(noSemicolon))
                .withMessage(invalidFormatMessage);

        String oneSemiColon = "0:";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseMessageIdString(oneSemiColon))
                .withMessage(invalidFormatMessage);

        String oneSemiColonComplete = "0:0";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseMessageIdString(oneSemiColonComplete))
                .withMessage(invalidFormatMessage);

        String twoSemiColon = "0:0:";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseMessageIdString(twoSemiColon))
                .withMessage(invalidNumberMessage);

        String twoSemiColonComplete = "0:0:0";
        assertThatNoException().isThrownBy(() -> parseMessageIdString(twoSemiColonComplete));

        String threeSemicolon = "0:0:0:";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseMessageIdString(threeSemicolon))
                .withMessage(invalidNumberMessage);

        String threeSemicolonComplete = "0:0:0:0";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseMessageIdString(threeSemicolonComplete))
                .withMessage(invalidNumberMessage);

        String invalidNumber = "0:0:adf";
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> parseMessageIdString(invalidNumber))
                .withMessage(invalidNumberMessage);
    }

    // --------------------------------------------------------------------------------------------
    // Table Sink Option Utils Test
    // --------------------------------------------------------------------------------------------

    @Test
    void topicRouter() {
        final Map<String, String> options = createDefaultOptions();
        options.put(
                SINK_CUSTOM_TOPIC_ROUTER.key(),
                "org.apache.flink.connector.pulsar.table.testutils.MockTopicRouter");
        TopicRouter<RowData> topicRouter =
                getTopicRouter(Configuration.fromMap(options), FactoryMocks.class.getClassLoader());
        assertThat(topicRouter).isInstanceOf(MockTopicRouter.class);

        options.put(
                SINK_CUSTOM_TOPIC_ROUTER.key(),
                "org.apache.flink.connector.pulsar.table.PulsarTableOptionsTest");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(
                        () ->
                                getTopicRouter(
                                        Configuration.fromMap(options),
                                        FactoryMocks.class.getClassLoader()))
                .withMessage(
                        String.format(
                                "Sink TopicRouter class '%s' should extend from the required class %s",
                                PulsarTableOptionsTest.class.getName(),
                                TopicRouter.class.getName()));

        options.put(
                SINK_CUSTOM_TOPIC_ROUTER.key(),
                "org.apache.flink.connector.pulsar.table.testutils.NonExistMockTopicRouter");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(
                        () ->
                                getTopicRouter(
                                        Configuration.fromMap(options),
                                        FactoryMocks.class.getClassLoader()))
                .withMessage(
                        String.format(
                                "Could not find and instantiate TopicRouter class '%s'",
                                "org.apache.flink.connector.pulsar.table.testutils.NonExistMockTopicRouter"));
    }

    @Test
    void topicRoutingMode() {
        final Map<String, String> options = createDefaultOptions();
        options.put(SINK_TOPIC_ROUTING_MODE.key(), "round-robin");
        TopicRoutingMode topicRoutingMode = getTopicRoutingMode(Configuration.fromMap(options));
        assertThat(topicRoutingMode).isEqualTo(TopicRoutingMode.ROUND_ROBIN);

        options.put(SINK_TOPIC_ROUTING_MODE.key(), "message-key-hash");
        topicRoutingMode = getTopicRoutingMode(Configuration.fromMap(options));
        assertThat(topicRoutingMode).isEqualTo(TopicRoutingMode.MESSAGE_KEY_HASH);
    }

    @Test
    void messageDelayMillis() {
        final Map<String, String> options = createDefaultOptions();
        options.put(SINK_MESSAGE_DELAY_INTERVAL.key(), "10 s");
        long messageDelayMillis = getMessageDelayMillis(Configuration.fromMap(options));
        assertThat(messageDelayMillis).isEqualTo(Duration.ofSeconds(10).toMillis());

        options.put(SINK_MESSAGE_DELAY_INTERVAL.key(), "10s");
        messageDelayMillis = getMessageDelayMillis(Configuration.fromMap(options));
        assertThat(messageDelayMillis).isEqualTo(Duration.ofSeconds(10).toMillis());

        options.put(SINK_MESSAGE_DELAY_INTERVAL.key(), "1000ms");
        messageDelayMillis = getMessageDelayMillis(Configuration.fromMap(options));
        assertThat(messageDelayMillis).isEqualTo(Duration.ofMillis(1000).toMillis());

        options.put(SINK_MESSAGE_DELAY_INTERVAL.key(), "1 d");
        messageDelayMillis = getMessageDelayMillis(Configuration.fromMap(options));
        assertThat(messageDelayMillis).isEqualTo(Duration.ofDays(1).toMillis());

        options.put(SINK_MESSAGE_DELAY_INTERVAL.key(), "1 H");
        messageDelayMillis = getMessageDelayMillis(Configuration.fromMap(options));
        assertThat(messageDelayMillis).isEqualTo(Duration.ofHours(1).toMillis());

        options.put(SINK_MESSAGE_DELAY_INTERVAL.key(), "1 min");
        messageDelayMillis = getMessageDelayMillis(Configuration.fromMap(options));
        assertThat(messageDelayMillis).isEqualTo(Duration.ofMinutes(1).toMillis());

        options.put(SINK_MESSAGE_DELAY_INTERVAL.key(), "1m");
        messageDelayMillis = getMessageDelayMillis(Configuration.fromMap(options));
        assertThat(messageDelayMillis).isEqualTo(Duration.ofMinutes(1).toMillis());
    }

    private Map<String, String> createDefaultOptions() {
        Map<String, String> optionMap = new HashMap<>();
        return optionMap;
    }
}
