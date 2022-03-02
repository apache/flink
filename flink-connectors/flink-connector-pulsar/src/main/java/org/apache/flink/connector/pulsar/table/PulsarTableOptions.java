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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;
import org.apache.flink.connector.pulsar.source.enumerator.topic.range.RangeGenerator;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.FORMAT_SUFFIX;

// TODO need to rewrite the entire class.
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

    // TODO these are the startCursor related options
    public static final ConfigOption<String> SCAN_STARTUP_MODE =
            ConfigOptions.key("scan.startup.mode")
                    .stringType()
                    .defaultValue("latest")
                    .withDescription(
                            "Optional startup mode for Pulsar consumer, valid enumerations are "
                                    + "\"earliest\", \"latest\", \"external-subscription\",\n"
                                    + "or \"specific-offsets\"");

    public static final ConfigOption<String> SCAN_STARTUP_SPECIFIC_OFFSETS =
            ConfigOptions.key("scan.startup.specific-offsets")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional offsets used in case of \"specific-offsets\" startup mode");

    public static final ConfigOption<Long> SCAN_STARTUP_TIMESTAMP_MILLIS =
            ConfigOptions.key("scan.startup.timestamp-millis")
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

    public static final ConfigOption<Integer> SINK_PARALLELISM = FactoryUtil.SINK_PARALLELISM;

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    // Start up offset.
    public static final String SCAN_STARTUP_MODE_VALUE_EARLIEST = "earliest";
    public static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest";

    private static final Set<String> SCAN_STARTUP_MODE_ENUMS =
            new HashSet<>(
                    Arrays.asList(
                            SCAN_STARTUP_MODE_VALUE_EARLIEST, SCAN_STARTUP_MODE_VALUE_LATEST));

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    // TODO will the cast lose context ? Enum and Map type ?
    // TODO by default it only supports string ConfigOptions
    public static Properties getPulsarProperties(ReadableConfig tableOptions) {
        final Properties pulsarProperties = new Properties();
        final Map<String, String> configs = ((Configuration) tableOptions).toMap();
        configs.keySet().stream()
                .filter(key -> key.startsWith("pulsar"))
                .forEach(key -> pulsarProperties.put(key, configs.get(key)));
        return pulsarProperties;
    }

    // TODO implement this
    public static Optional<RangeGenerator> getRangeGenerator(ReadableConfig tableOptions) {
        return Optional.empty();
    }

    public static Optional<StartCursor> getStartCursor(ReadableConfig tableOptions) {
        return Optional.empty();
    }
}
