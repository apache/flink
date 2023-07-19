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

package org.apache.flink.formats.json;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

/** Options for the JSON format. */
@PublicEvolving
public class JsonFormatOptions {

    public static final ConfigOption<Boolean> FAIL_ON_MISSING_FIELD =
            ConfigOptions.key("fail-on-missing-field")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to fail if a field is missing or not, false by default.");

    public static final ConfigOption<Boolean> IGNORE_PARSE_ERRORS =
            ConfigOptions.key("ignore-parse-errors")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to skip fields and rows with parse errors instead of failing;\n"
                                    + "fields are set to null in case of errors, false by default.");

    public static final ConfigOption<String> MAP_NULL_KEY_MODE =
            ConfigOptions.key("map-null-key.mode")
                    .stringType()
                    .defaultValue("FAIL")
                    .withDescription(
                            "Optional flag to control the handling mode when serializing null key for map data, FAIL by default."
                                    + " Option DROP will drop null key entries for map data."
                                    + " Option LITERAL will use 'map-null-key.literal' as key literal.");

    public static final ConfigOption<String> MAP_NULL_KEY_LITERAL =
            ConfigOptions.key("map-null-key.literal")
                    .stringType()
                    .defaultValue("null")
                    .withDescription(
                            "Optional flag to specify string literal for null keys when 'map-null-key.mode' is LITERAL, \"null\" by default.");

    public static final ConfigOption<String> TIMESTAMP_FORMAT =
            ConfigOptions.key("timestamp-format.standard")
                    .stringType()
                    .defaultValue("SQL")
                    .withDescription(
                            "Optional flag to specify timestamp format, SQL by default."
                                    + " Option ISO-8601 will parse input timestamp in \"yyyy-MM-ddTHH:mm:ss.s{precision}\" format and output timestamp in the same format."
                                    + " Option SQL will parse input timestamp in \"yyyy-MM-dd HH:mm:ss.s{precision}\" format and output timestamp in the same format.");

    public static final ConfigOption<Boolean> ENCODE_DECIMAL_AS_PLAIN_NUMBER =
            ConfigOptions.key("encode.decimal-as-plain-number")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Optional flag to specify whether to encode all decimals as plain numbers instead of possible scientific notations, false by default.");

    public static final ConfigOption<Boolean> DECODE_JSON_PARSER_ENABLED =
            ConfigOptions.key("decode.json-parser.enabled")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Optional flag to specify whether to use the Jackson JsonParser to decode json with better performance, true by default.");

    // --------------------------------------------------------------------------------------------
    // Enums
    // --------------------------------------------------------------------------------------------

    /** Handling mode for map data with null key. */
    public enum MapNullKeyMode {
        FAIL,
        DROP,
        LITERAL
    }

    private JsonFormatOptions() {}
}
