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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.formats.json.JsonFormatOptions.FAIL_ON_MISSING_FIELD;
import static org.apache.flink.formats.json.JsonFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.json.JsonFormatOptions.MAP_NULL_KEY_MODE;
import static org.apache.flink.formats.json.JsonFormatOptions.TIMESTAMP_FORMAT;

/** Utilities for {@link JsonFormatOptions}. */
@Internal
public class JsonFormatOptionsUtil {

    // --------------------------------------------------------------------------------------------
    // Option enumerations
    // --------------------------------------------------------------------------------------------

    public static final String SQL = "SQL";
    public static final String ISO_8601 = "ISO-8601";

    public static final Set<String> TIMESTAMP_FORMAT_ENUM =
            new HashSet<>(Arrays.asList(SQL, ISO_8601));

    // The handling mode of null key for map data
    public static final String JSON_MAP_NULL_KEY_MODE_FAIL = "FAIL";
    public static final String JSON_MAP_NULL_KEY_MODE_DROP = "DROP";
    public static final String JSON_MAP_NULL_KEY_MODE_LITERAL = "LITERAL";

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static TimestampFormat getTimestampFormat(ReadableConfig config) {
        String timestampFormat = config.get(TIMESTAMP_FORMAT);
        switch (timestampFormat) {
            case SQL:
                return TimestampFormat.SQL;
            case ISO_8601:
                return TimestampFormat.ISO_8601;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported timestamp format '%s'. Validator should have checked that.",
                                timestampFormat));
        }
    }

    /**
     * Creates handling mode for null key map data.
     *
     * <p>See {@link #JSON_MAP_NULL_KEY_MODE_FAIL}, {@link #JSON_MAP_NULL_KEY_MODE_DROP}, and {@link
     * #JSON_MAP_NULL_KEY_MODE_LITERAL} for more information.
     */
    public static JsonFormatOptions.MapNullKeyMode getMapNullKeyMode(ReadableConfig config) {
        String mapNullKeyMode = config.get(MAP_NULL_KEY_MODE);
        switch (mapNullKeyMode.toUpperCase()) {
            case JSON_MAP_NULL_KEY_MODE_FAIL:
                return JsonFormatOptions.MapNullKeyMode.FAIL;
            case JSON_MAP_NULL_KEY_MODE_DROP:
                return JsonFormatOptions.MapNullKeyMode.DROP;
            case JSON_MAP_NULL_KEY_MODE_LITERAL:
                return JsonFormatOptions.MapNullKeyMode.LITERAL;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported map null key handling mode '%s'. Validator should have checked that.",
                                mapNullKeyMode));
        }
    }

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    /** Validator for json decoding format. */
    public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
        boolean failOnMissingField = tableOptions.get(FAIL_ON_MISSING_FIELD);
        boolean ignoreParseErrors = tableOptions.get(IGNORE_PARSE_ERRORS);
        if (ignoreParseErrors && failOnMissingField) {
            throw new ValidationException(
                    FAIL_ON_MISSING_FIELD.key()
                            + " and "
                            + IGNORE_PARSE_ERRORS.key()
                            + " shouldn't both be true.");
        }
        validateTimestampFormat(tableOptions);
    }

    /** Validator for json encoding format. */
    public static void validateEncodingFormatOptions(ReadableConfig tableOptions) {
        // validator for {@link MAP_NULL_KEY_MODE}
        Set<String> nullKeyModes =
                Arrays.stream(JsonFormatOptions.MapNullKeyMode.values())
                        .map(Objects::toString)
                        .collect(Collectors.toSet());
        if (!nullKeyModes.contains(tableOptions.get(MAP_NULL_KEY_MODE).toUpperCase())) {
            throw new ValidationException(
                    String.format(
                            "Unsupported value '%s' for option %s. Supported values are %s.",
                            tableOptions.get(MAP_NULL_KEY_MODE),
                            MAP_NULL_KEY_MODE.key(),
                            nullKeyModes));
        }
        validateTimestampFormat(tableOptions);
    }

    /** Validates timestamp format which value should be SQL or ISO-8601. */
    static void validateTimestampFormat(ReadableConfig tableOptions) {
        String timestampFormat = tableOptions.get(TIMESTAMP_FORMAT);
        if (!TIMESTAMP_FORMAT_ENUM.contains(timestampFormat)) {
            throw new ValidationException(
                    String.format(
                            "Unsupported value '%s' for %s. Supported values are [SQL, ISO-8601].",
                            timestampFormat, TIMESTAMP_FORMAT.key()));
        }
    }

    private JsonFormatOptionsUtil() {}
}
