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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import org.apache.pulsar.client.api.SubscriptionType;

import java.util.List;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

/**
 * Config options that is used to configure a Pulsar SQL Connector. These config options are
 * specific to SQL Connectors only. Other runtime configurations can be found in {@link
 * org.apache.flink.connector.pulsar.common.config.PulsarOptions}, {@link
 * org.apache.flink.connector.pulsar.source.PulsarSourceOptions}, and {@link
 * org.apache.flink.connector.pulsar.sink.PulsarSinkOptions}.
 */
public class PulsarTableOptions {
    // TODO these two will be kept
    public static final ConfigOption<List<String>> TOPIC =
            ConfigOptions.key("topic")
                    .stringType()
                    .asList()
                    .noDefaultValue()
                    .withDescription(
                            "Topic names from which the table is read. Either 'topic' or 'topic-pattern' must be set for source. "
                                    + "Option 'topic' is required for sink.");

    // TODO these are the startCursor related options, add description for it.
    public static final ConfigOption<SubscriptionType> SOURCE_SOURCE_SUBSCRIPTION_TYPE =
            ConfigOptions.key("source.subscription-type")
                    .enumType(SubscriptionType.class)
                    .defaultValue(SubscriptionType.Exclusive)
                    .withDescription("");

    public static final ConfigOption<String> SOURCE_START_FROM_MESSAGE_ID =
            ConfigOptions.key("source.start.message-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional offsets used in case of \"specific-offsets\" startup mode");

    public static final ConfigOption<Long> SOURCE_START_FROM_PUBLISH_TIME =
            ConfigOptions.key("source.start.publish-time")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional timestamp used in case of \"timestamp\" startup mode");

    // --------------------------------------------------------------------------------------------
    // Format enumerations
    // --------------------------------------------------------------------------------------------

    public static final ConfigOption<String> KEY_FORMAT =
            ConfigOptions.key("key" + FORMAT_SUFFIX)
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Defines the format identifier for encoding key data. "
                                    + "The identifier is used to discover a suitable format factory.");

    public static final ConfigOption<List<String>> KEY_FIELDS =
            ConfigOptions.key("key.fields")
                    .stringType()
                    .asList()
                    .defaultValues()
                    .withDescription(
                            "Defines an explicit list of physical columns from the table schema "
                                    + "that configure the data type for the key format. By default, this list is "
                                    + "empty and thus a key is undefined.");

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    // Start up offset.
    public static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest";
    public static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest";
}
