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

import org.apache.pulsar.client.impl.auth.AuthenticationToken;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAM_MAP;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_PROXY_PROTOCOL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_INITIAL_POSITION;
import static org.assertj.core.api.Assertions.assertThatNoException;

/** Test config options for Pulsar SQL connector. */
public class PulsarTableConfigTest extends PulsarTableTestBase {

    @Test
    void sourceSubscriptionType() {
        String testConfigString = " 'source.subscription-type' = 'Exclusive' ";
        runSql(testConfigString);
    }

    @Test
    void sourceMessageIdStartCursor() {
        String testConfigString = " 'source.start.message-id' = 'earliest' ";
        runSql(testConfigString);

        testConfigString = " 'source.start.message-id' = 'latest' ";
        runSql(testConfigString);

        testConfigString = " 'source.start.message-id' = '0:0:-1' ";
        runSql(testConfigString);
    }

    @Test
    void sourceTimestampStartCursor() {
        String testConfigString = " 'source.start.publish-time' = '233010230' ";
        runSql(testConfigString);
    }

    // TODO not verified yet, can it create the enum ?
    @Test
    void pulsarSourceOptionsSubscriptionInitialPosition() {
        Map<String, String> testConfigs = new HashMap<>();
        testConfigs.put(PULSAR_SUBSCRIPTION_INITIAL_POSITION.key(), "Earliest");
        runSql(createTestConfig(testConfigs));
    }

    @Test
    void pulsarOptionsAuthParamMap() {
        Map<String, String> testConfigs = new HashMap<>();
        testConfigs.put(PULSAR_AUTH_PARAM_MAP.key(), "key1:value1,key2:value2");
        testConfigs.put(PULSAR_AUTH_PLUGIN_CLASS_NAME.key(), AuthenticationToken.class.getName());
        runSql(createTestConfig(testConfigs));
    }

    @Test
    void invalidSourceTimestampStartCursor() {
        String testConfigString = " 'source.start.message-id' = '0:0:' ";
        runSql(testConfigString);
    }

    private String createTestConfig(Map<String, String> configMap) {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : configMap.entrySet()) {
            sb.append(String.format(" '%s' = '%s' ,\n", entry.getKey(), entry.getValue()));
        }
        String configStr = sb.toString();
        return configStr.substring(0, configStr.length() - 2);
    }

    private void runSql(String testConfigString) {
        final String topic = "config_test_topic" + randomAlphanumeric(3);
        final String randomTableName = randomAlphabetic(5);
        createTestTopic(topic, 1);
        final String createTable =
                String.format(
                        "CREATE TABLE %s (\n"
                                + "  `physical_1` STRING,\n"
                                + "  `physical_2` INT,\n"
                                + "  `physical_3` BOOLEAN\n"
                                + ") WITH (\n"
                                + "  'connector' = 'pulsar',\n"
                                + "  'topic' = '%s',\n"
                                + "  'service-url' = '%s',\n"
                                + "  'admin-url' = '%s',\n"
                                + "  'format' = 'json',\n"
                                + "  %s"
                                + ")",
                        randomTableName,
                        topic,
                        pulsar.operator().serviceUrl(),
                        pulsar.operator().adminUrl(),
                        testConfigString);
        tableEnv.executeSql(createTable);
        String initialValues =
                String.format(
                        "INSERT INTO %s\n"
                                + "VALUES\n"
                                + " ('data 1', 1, TRUE),\n"
                                + " ('data 2', 2, FALSE),\n"
                                + " ('data 3', 3, TRUE)",
                        randomTableName);
        assertThatNoException().isThrownBy(() -> tableEnv.executeSql(initialValues).await());
        tableEnv.sqlQuery(String.format("SELECT * FROM %s", randomTableName));
    }
}
