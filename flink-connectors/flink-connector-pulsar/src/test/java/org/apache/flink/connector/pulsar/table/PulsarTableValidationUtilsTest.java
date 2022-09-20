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
import org.apache.flink.table.api.ValidationException;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FIELDS;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.KEY_FORMAT;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_CUSTOM_TOPIC_ROUTER;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SINK_TOPIC_ROUTING_MODE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_MESSAGE_ID;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_START_FROM_PUBLISH_TIME;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.SOURCE_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.table.PulsarTableOptions.TOPICS;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validateKeyFormatConfigs;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validateSinkRoutingConfigs;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validateStartCursorConfigs;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validateSubscriptionTypeConfigs;
import static org.apache.flink.connector.pulsar.table.PulsarTableValidationUtils.validateTopicsConfigs;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatNoException;

/** Unit test for {@link PulsarTableValidationUtils}. */
public class PulsarTableValidationUtilsTest extends PulsarTableTestBase {
    @Test
    void topicsConfigs() {
        final Map<String, String> options = createDefaultOptions();
        options.put(TOPICS.key(), "");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> validateTopicsConfigs(Configuration.fromMap(options)))
                .withMessage("The topics list should not be empty.");

        String invalidTopicName = "persistent://tenant/topic";
        String validTopicName = "valid-topic";

        options.put(TOPICS.key(), invalidTopicName);
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> validateTopicsConfigs(Configuration.fromMap(options)))
                .withMessage(
                        String.format(
                                "The topics name %s is not a valid topic name.", invalidTopicName));

        options.put(TOPICS.key(), validTopicName + ";" + invalidTopicName);
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> validateTopicsConfigs(Configuration.fromMap(options)))
                .withMessage(
                        String.format(
                                "The topics name %s is not a valid topic name.", invalidTopicName));

        options.put(TOPICS.key(), validTopicName + ";" + validTopicName);
        assertThatNoException()
                .isThrownBy(() -> validateTopicsConfigs(Configuration.fromMap(options)));

        options.put(TOPICS.key(), validTopicName + ";");
        assertThatNoException()
                .isThrownBy(() -> validateTopicsConfigs(Configuration.fromMap(options)));
    }

    @Test
    void startCursorConfigs() {
        final Map<String, String> options = createDefaultOptions();
        options.put(SOURCE_START_FROM_MESSAGE_ID.key(), "latest");
        assertThatNoException()
                .isThrownBy(() -> validateStartCursorConfigs(Configuration.fromMap(options)));

        options.remove(SOURCE_START_FROM_MESSAGE_ID.key());
        options.put(SOURCE_START_FROM_PUBLISH_TIME.key(), "2345123234");
        assertThatNoException()
                .isThrownBy(() -> validateStartCursorConfigs(Configuration.fromMap(options)));

        options.put(SOURCE_START_FROM_MESSAGE_ID.key(), "latest");
        options.put(SOURCE_START_FROM_PUBLISH_TIME.key(), "2345123234");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> validateStartCursorConfigs(Configuration.fromMap(options)))
                .withMessage(
                        String.format(
                                "Only one of %s and %s can be specified. Detected both of them",
                                SOURCE_START_FROM_MESSAGE_ID, SOURCE_START_FROM_PUBLISH_TIME));
    }

    @Test
    void subscriptionTypeConfigs() {
        final Map<String, String> options = createDefaultOptions();

        options.put(SOURCE_SUBSCRIPTION_TYPE.key(), "Exclusive");
        assertThatNoException()
                .isThrownBy(() -> validateSubscriptionTypeConfigs(Configuration.fromMap(options)));

        options.put(SOURCE_SUBSCRIPTION_TYPE.key(), "Shared");
        assertThatNoException()
                .isThrownBy(() -> validateSubscriptionTypeConfigs(Configuration.fromMap(options)));

        options.put(SOURCE_SUBSCRIPTION_TYPE.key(), "Key_Shared");
        assertThatNoException()
                .isThrownBy(() -> validateSubscriptionTypeConfigs(Configuration.fromMap(options)));

        options.put(SOURCE_SUBSCRIPTION_TYPE.key(), "invalid-subscription");
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> validateSubscriptionTypeConfigs(Configuration.fromMap(options)))
                .withMessage(
                        "Could not parse value 'invalid-subscription' for key 'source.subscription-type'.");
    }

    @Test
    void sinkRoutingConfigs() {
        final Map<String, String> options = createDefaultOptions();
        options.put(SINK_TOPIC_ROUTING_MODE.key(), "round-robin");
        assertThatNoException()
                .isThrownBy(() -> validateSinkRoutingConfigs(Configuration.fromMap(options)));

        // validation does not try to create the class
        options.remove(SINK_TOPIC_ROUTING_MODE.key());
        options.put(SINK_CUSTOM_TOPIC_ROUTER.key(), "invalid-class-name");
        assertThatNoException()
                .isThrownBy(() -> validateSinkRoutingConfigs(Configuration.fromMap(options)));

        options.put(SINK_TOPIC_ROUTING_MODE.key(), "round-robin");
        options.put(SINK_CUSTOM_TOPIC_ROUTER.key(), "invalid-class-name");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> validateSinkRoutingConfigs(Configuration.fromMap(options)))
                .withMessage(
                        String.format(
                                "Only one of %s and %s can be specified. Detected both of them",
                                SINK_CUSTOM_TOPIC_ROUTER, SINK_TOPIC_ROUTING_MODE));
    }

    @Test
    void keyFormatConfigs() {
        final Map<String, String> options = createDefaultOptions();
        options.put(KEY_FIELDS.key(), "");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> validateKeyFormatConfigs(Configuration.fromMap(options)))
                .withMessage(
                        String.format(
                                "The option '%s' can only be declared if a key format is defined using '%s'.",
                                KEY_FIELDS.key(), KEY_FORMAT.key()));

        options.put(KEY_FORMAT.key(), "json");
        options.remove(KEY_FIELDS.key());
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> validateKeyFormatConfigs(Configuration.fromMap(options)))
                .withMessage(
                        String.format(
                                "A key format '%s' requires the declaration of one or more of key fields using '%s'.",
                                KEY_FORMAT.key(), KEY_FIELDS.key()));

        options.put(KEY_FORMAT.key(), "json");
        options.put(KEY_FIELDS.key(), "");
        assertThatExceptionOfType(ValidationException.class)
                .isThrownBy(() -> validateKeyFormatConfigs(Configuration.fromMap(options)))
                .withMessage(
                        String.format(
                                "A key format '%s' requires the declaration of one or more of key fields using '%s'.",
                                KEY_FORMAT.key(), KEY_FIELDS.key()));

        options.put(KEY_FORMAT.key(), "json");
        options.put(KEY_FIELDS.key(), "k_field1");
        assertThatNoException()
                .isThrownBy(() -> validateKeyFormatConfigs(Configuration.fromMap(options)));
    }

    private Map<String, String> createDefaultOptions() {
        Map<String, String> optionMap = new HashMap<>();
        return optionMap;
    }
}
