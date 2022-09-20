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

import org.apache.flink.connector.pulsar.table.testutils.MockPulsarAuthentication;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.factories.FactoryUtil;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAM_MAP;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME;
import static org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils.topicNameWithPartition;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_CUSTOM_TOPIC_ROUTER;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_MESSAGE_DELAY_INTERVAL;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_TOPIC_ROUTING_MODE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_STOP_AT_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_STOP_AT_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.TOPICS;
import static org.apache.flink.table.factories.TestDynamicTableFactory.VALUE_FORMAT;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

/**
 * Test config options for Pulsar SQL connector. This test aims to verify legal combination of
 * config options will be accepted and do not cause runtime exceptions (but cannot guarantee they
 * are taking effect), and illegal combinations of config options will be rejected early.
 */
public class PulsarTableOptionsTest extends PulsarTableTestBase {
    @Test
    void noTopicsSpecified() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithFormat();
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectException(
                topicName,
                new ValidationException(
                        "One or more required options are missing.\n\n"
                                + "Missing required options are:\n\n"
                                + "topics"));
    }

    @Test
    void invalidEmptyTopics() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithFormat();
        testConfigs.put(TOPICS.key(), "");
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectException(
                topicName, new ValidationException("The topics list should not be empty."));
    }

    @Test
    void topicsWithSemicolon() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithFormat();
        testConfigs.put(TOPICS.key(), topicName + ";");
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void invalidTopicName() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithFormat();
        String invalidTopicName = "persistent://tenant/no-topic";
        testConfigs.put(TOPICS.key(), invalidTopicName);
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectException(
                topicName,
                new ValidationException(
                        String.format(
                                "The topics name %s is not a valid topic name.",
                                invalidTopicName)));
    }

    @Test
    void topicsList() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithFormat();
        testConfigs.put(
                TOPICS.key(),
                topicNameWithPartition(topicName, 0) + ";" + topicNameWithPartition(topicName, 1));
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void usingFormat() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopic(topicName);
        testConfigs.put(FactoryUtil.FORMAT.key(), "json");
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void usingValueFormat() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopic(topicName);
        testConfigs.put(VALUE_FORMAT.key(), "json");
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void usingValueFormatAndFormatOptions() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopic(topicName);
        testConfigs.put(VALUE_FORMAT.key(), "json");
        testConfigs.put("value.json.fail-on-missing-field", "false");
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void subscriptionType() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);
        testConfigs.put(SOURCE_SUBSCRIPTION_TYPE.key(), "Exclusive");
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void invalidUnsupportedSubscriptionType() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);
        testConfigs.put(SOURCE_SUBSCRIPTION_TYPE.key(), "Failover");
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectException(
                topicName, new ValidationException("Failover SubscriptionType is not supported. "));
    }

    @Test
    void invalidNonExistSubscriptionType() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);
        testConfigs.put(SOURCE_SUBSCRIPTION_TYPE.key(), "random-subscription-type");
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectException(
                topicName,
                new ValidationException("Invalid value for option 'source.subscription-type'."));
    }

    @Test
    void messageIdStartCursorEarliest() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SOURCE_START_FROM_MESSAGE_ID.key(), "earliest");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void messageIdStartCursorLatest() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SOURCE_START_FROM_MESSAGE_ID.key(), "latest");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void messageIdStartCursorExact() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SOURCE_START_FROM_MESSAGE_ID.key(), "0:0:-1");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void invalidMessageIdStartCursorEmptyId() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SOURCE_START_FROM_MESSAGE_ID.key(), "0:0:");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectException(
                topicName,
                new IllegalArgumentException(
                        "MessageId format must be ledgerId:entryId:partitionId. "
                                + "Each id should be able to parsed to long type."));
    }

    @Test
    void invalidMessageIdStartCursorIncomplete() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SOURCE_START_FROM_MESSAGE_ID.key(), "0:0");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectException(
                topicName,
                new IllegalArgumentException(
                        "MessageId format must be ledgerId:entryId:partitionId."));
    }

    @Test
    void timestampStartCursor() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SOURCE_START_FROM_PUBLISH_TIME.key(), "233010230");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void messageIdStopCursorNever() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SOURCE_STOP_AT_MESSAGE_ID.key(), "never");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void messageIdStopCursorLatest() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SOURCE_STOP_AT_MESSAGE_ID.key(), "latest");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void messageIdStopCursorExact() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SOURCE_STOP_AT_MESSAGE_ID.key(), "0:0:-1");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void timestampStopCursor() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SOURCE_STOP_AT_PUBLISH_TIME.key(), "233010230");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void topicRoutingMode() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SINK_TOPIC_ROUTING_MODE.key(), "message-key-hash");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void invalidTopicRouter() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        String invalidClassName = "invalid class name";
        testConfigs.put(SINK_CUSTOM_TOPIC_ROUTER.key(), invalidClassName);

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectException(
                topicName,
                new ValidationException(
                        String.format(
                                "Could not find and instantiate TopicRouter class '%s'",
                                invalidClassName)));
    }

    @Test
    void messageDelay() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SINK_MESSAGE_DELAY_INTERVAL.key(), "10s");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    @Test
    void invalidMessageDelay() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(SINK_MESSAGE_DELAY_INTERVAL.key(), "invalid-duration");

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectException(
                topicName,
                new ValidationException("Invalid value for option 'sink.message-delay-interval'."));
    }

    // --------------------------------------------------------------------------------------------
    // PulsarOptions, PulsarSourceOptions, PulsarSinkOptions  Test
    // --------------------------------------------------------------------------------------------

    @Test
    void pulsarOptionsAuthParamMap() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put(PULSAR_AUTH_PARAM_MAP.key(), "key1:value1,key2:value2");
        testConfigs.put(
                PULSAR_AUTH_PLUGIN_CLASS_NAME.key(), MockPulsarAuthentication.class.getName());

        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectSucceed(topicName);
        runSourceAndExpectSucceed(topicName);
    }

    // --------------------------------------------------------------------------------------------
    // requiredOptions(), optionalOptions()  Test
    // --------------------------------------------------------------------------------------------

    @Test
    void unusedConfigOptions() {
        final String topicName = randomTopicName();
        Map<String, String> testConfigs = testConfigWithTopicAndFormat(topicName);

        testConfigs.put("random_config", "random_value");
        runSql(topicName, createTestConfig(testConfigs));
        runSinkAndExpectException(topicName, ValidationException.class);
    }

    // --------------------------------------------------------------------------------------------
    // Utils methods
    // --------------------------------------------------------------------------------------------

    private String createTestConfig(Map<String, String> configMap) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            sb.append(String.format(" '%s' = '%s' ,\n", entry.getKey(), entry.getValue()));
        }
        return sb.toString();
    }

    private void runSql(String topicName, String testConfigString) {
        createTestTopic(topicName, 2);
        final String createTable =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  `physical_1` STRING,\n"
                                + "  `physical_2` INT,\n"
                                + "  `physical_3` BOOLEAN\n"
                                + ") WITH (\n"
                                + "  'service-url' = '%s',\n"
                                + "  'admin-url' = '%s',\n"
                                + "  %s\n"
                                + "  'connector' = 'pulsar'"
                                + ")",
                        topicName,
                        pulsar.operator().serviceUrl(),
                        pulsar.operator().adminUrl(),
                        testConfigString);
        tableEnv.executeSql(createTable);
    }

    private void runSinkAndExpectSucceed(String topicName) {
        String initialValues =
                String.format(
                        "INSERT INTO %s\n"
                                + "VALUES\n"
                                + " ('data 1', 1, TRUE),\n"
                                + " ('data 2', 2, FALSE),\n"
                                + " ('data 3', 3, TRUE)",
                        topicName);
        assertThatNoException().isThrownBy(() -> tableEnv.executeSql(initialValues).await());
    }

    private void runSinkAndExpectException(
            String topicName, final Class<? extends Throwable> exceptionType) {
        String initialValues =
                String.format(
                        "INSERT INTO %s\n"
                                + "VALUES\n"
                                + " ('data 1', 1, TRUE),\n"
                                + " ('data 2', 2, FALSE),\n"
                                + " ('data 3', 3, TRUE)",
                        topicName);
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> tableEnv.executeSql(initialValues).await());
    }

    private void runSinkAndExpectException(String topicName, Throwable cause) {
        String initialValues =
                String.format(
                        "INSERT INTO %s\n"
                                + "VALUES\n"
                                + " ('data 1', 1, TRUE),\n"
                                + " ('data 2', 2, FALSE),\n"
                                + " ('data 3', 3, TRUE)",
                        topicName);
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> tableEnv.executeSql(initialValues).await())
                .withCause(cause);
    }

    private void runSourceAndExpectSucceed(String topicName) {
        assertThatNoException()
                .isThrownBy(() -> tableEnv.sqlQuery(String.format("SELECT * FROM %s", topicName)));
    }

    private void runSourceAndExpectException(String topicName, Throwable cause) {
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> tableEnv.sqlQuery(String.format("SELECT * FROM %s", topicName)))
                .withCause(cause);
    }

    private String randomTopicName() {
        final String testTopicPrefix = "test_config_topic";
        return testTopicPrefix + randomAlphabetic(5);
    }

    private Map<String, String> testConfigWithTopicAndFormat(String tableName) {
        Map<String, String> testConfigs = new HashMap<>();
        testConfigs.put(TOPICS.key(), tableName);
        testConfigs.put(FactoryUtil.FORMAT.key(), "json");
        return testConfigs;
    }

    private Map<String, String> testConfigWithFormat() {
        Map<String, String> testConfigs = new HashMap<>();
        testConfigs.put(FactoryUtil.FORMAT.key(), "json");
        return testConfigs;
    }

    private Map<String, String> testConfigWithTopic(String tableName) {
        Map<String, String> testConfigs = new HashMap<>();
        testConfigs.put(TOPICS.key(), tableName);
        return testConfigs;
    }
}
