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
import static org.apache.flink.formats.json.JsonFormatOptions.ZERO_TIMESTAMP_BEHAVIOR;

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

    // The handling mode of zero datetime for Timestamp data
    public static final String JSON_ZERO_TIMESTAMP_BEHAVIOR_FAIL = "FAIL";
    public static final String JSON_ZERO_TIMESTAMP_BEHAVIOR_CONVERT_TO_NULL = "CONVERT_TO_NULL";

    public static final Set<String> ZERO_TIMESTAMP_BEHAVIOR_ENUM =
            new HashSet<>(
                    Arrays.asList(
                            JSON_ZERO_TIMESTAMP_BEHAVIOR_FAIL,
                            JSON_ZERO_TIMESTAMP_BEHAVIOR_CONVERT_TO_NULL));

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

    public static JsonFormatOptions.ZeroTimestampBehavior getZeroTimestampBehavior(
            ReadableConfig config) {
        String zeroTimestampBehavior = config.get(ZERO_TIMESTAMP_BEHAVIOR);
        switch (zeroTimestampBehavior) {
            case JSON_ZERO_TIMESTAMP_BEHAVIOR_FAIL:
                return JsonFormatOptions.ZeroTimestampBehavior.FAIL;
            case JSON_ZERO_TIMESTAMP_BEHAVIOR_CONVERT_TO_NULL:
                return JsonFormatOptions.ZeroTimestampBehavior.CONVERT_TO_NULL;
            default:
                throw new TableException(
                        String.format(
                                "Unsupported zero timestamp behavior '%s'. Validator should have checked that.",
                                zeroTimestampBehavior));
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
        validateZeroTimestampBehavior(tableOptions);
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

    /** Validates zero timestamp behavior which value should be FAIL or CONVERT_TO_NULL. */
    static void validateZeroTimestampBehavior(ReadableConfig tableOptions) {
        String zeroTimestampBehavior = tableOptions.get(ZERO_TIMESTAMP_BEHAVIOR);
        if (!ZERO_TIMESTAMP_BEHAVIOR_ENUM.contains(zeroTimestampBehavior)) {
            throw new ValidationException(
                    String.format(
                            "Unsupported value '%s' for %s. Supported values are [FAIL, CONVERT_TO_NULL].",
                            zeroTimestampBehavior, ZERO_TIMESTAMP_BEHAVIOR.key()));
        }
    }

    private JsonFormatOptionsUtil() {}
}
