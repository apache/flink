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

package org.apache.flink.formats.json.debezium;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.table.api.ValidationException;

import static org.apache.flink.formats.json.debezium.DebeziumJsonFormatFactory.IDENTIFIER;

/** Option utils for debezium-json format. */
public class DebeziumJsonOptions {

    public static final ConfigOption<Boolean> SCHEMA_INCLUDE =
            ConfigOptions.key("schema-include")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When setting up a Debezium Kafka Connect, users can enable "
                                    + "a Kafka configuration 'value.converter.schemas.enable' to include schema in the message. "
                                    + "This option indicates the Debezium JSON data include the schema in the message or not. "
                                    + "Default is false.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS = JsonOptions.IGNORE_PARSE_ERRORS;

    public static final ConfigOption<String> TIMESTAMP_FORMAT = JsonOptions.TIMESTAMP_FORMAT;

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_MODE = JsonOptions.MAP_NULL_KEY_MODE;

    public static final ConfigOption<String> JSON_MAP_NULL_KEY_LITERAL =
            JsonOptions.MAP_NULL_KEY_LITERAL;

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    /** Validator for debezium decoding format. */
    public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
        JsonOptions.validateDecodingFormatOptions(tableOptions);
    }

    /** Validator for debezium encoding format. */
    public static void validateEncodingFormatOptions(ReadableConfig tableOptions) {
        JsonOptions.validateEncodingFormatOptions(tableOptions);

        // validator for {@link SCHEMA_INCLUDE}
        if (tableOptions.get(SCHEMA_INCLUDE)) {
            throw new ValidationException(
                    String.format(
                            "Debezium JSON serialization doesn't support '%s.%s' option been set to true.",
                            IDENTIFIER, SCHEMA_INCLUDE.key()));
        }
    }
}
