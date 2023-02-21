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

package org.apache.flink.formats.csv;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;

import org.apache.commons.text.StringEscapeUtils;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.formats.csv.CsvFormatOptions.ALLOW_COMMENTS;
import static org.apache.flink.formats.csv.CsvFormatOptions.ARRAY_ELEMENT_DELIMITER;
import static org.apache.flink.formats.csv.CsvFormatOptions.DISABLE_QUOTE_CHARACTER;
import static org.apache.flink.formats.csv.CsvFormatOptions.ESCAPE_CHARACTER;
import static org.apache.flink.formats.csv.CsvFormatOptions.FIELD_DELIMITER;
import static org.apache.flink.formats.csv.CsvFormatOptions.IGNORE_PARSE_ERRORS;
import static org.apache.flink.formats.csv.CsvFormatOptions.NULL_LITERAL;
import static org.apache.flink.formats.csv.CsvFormatOptions.QUOTE_CHARACTER;
import static org.apache.flink.formats.csv.CsvFormatOptions.WRITE_BIGDECIMAL_IN_SCIENTIFIC_NOTATION;

/** A class with common CSV format constants and utility methods. */
class CsvCommons {

    public static final String IDENTIFIER = "csv";

    // ------------------------------------------------------------------------
    //  Validation
    // ------------------------------------------------------------------------

    static void validateFormatOptions(ReadableConfig tableOptions) {
        final boolean hasQuoteCharacter = tableOptions.getOptional(QUOTE_CHARACTER).isPresent();
        final boolean isDisabledQuoteCharacter = tableOptions.get(DISABLE_QUOTE_CHARACTER);
        if (isDisabledQuoteCharacter && hasQuoteCharacter) {
            throw new ValidationException(
                    "Format cannot define a quote character and disabled quote character at the same time.");
        }
        // Validate the option value must be a single char.
        validateCharacterVal(tableOptions, FIELD_DELIMITER, true);
        validateCharacterVal(tableOptions, ARRAY_ELEMENT_DELIMITER);
        validateCharacterVal(tableOptions, QUOTE_CHARACTER);
        validateCharacterVal(tableOptions, ESCAPE_CHARACTER);
    }

    /** Validates the option {@code option} value must be a Character. */
    private static void validateCharacterVal(
            ReadableConfig tableOptions, ConfigOption<String> option) {
        validateCharacterVal(tableOptions, option, false);
    }

    /**
     * Validates the option {@code option} value must be a Character.
     *
     * @param tableOptions the table options
     * @param option the config option
     * @param unescape whether to unescape the option value
     */
    private static void validateCharacterVal(
            ReadableConfig tableOptions, ConfigOption<String> option, boolean unescape) {
        if (!tableOptions.getOptional(option).isPresent()) {
            return;
        }

        final String value =
                unescape
                        ? StringEscapeUtils.unescapeJava(tableOptions.get(option))
                        : tableOptions.get(option);
        if (value.length() != 1) {
            throw new ValidationException(
                    String.format(
                            "Option '%s.%s' must be a string with single character, but was: %s",
                            IDENTIFIER, option.key(), tableOptions.get(option)));
        }
    }

    public static Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FIELD_DELIMITER);
        options.add(DISABLE_QUOTE_CHARACTER);
        options.add(QUOTE_CHARACTER);
        options.add(ALLOW_COMMENTS);
        options.add(IGNORE_PARSE_ERRORS);
        options.add(ARRAY_ELEMENT_DELIMITER);
        options.add(ESCAPE_CHARACTER);
        options.add(NULL_LITERAL);
        options.add(WRITE_BIGDECIMAL_IN_SCIENTIFIC_NOTATION);
        return options;
    }

    public static Set<ConfigOption<?>> forwardOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(FIELD_DELIMITER);
        options.add(DISABLE_QUOTE_CHARACTER);
        options.add(QUOTE_CHARACTER);
        options.add(ALLOW_COMMENTS);
        options.add(ARRAY_ELEMENT_DELIMITER);
        options.add(ESCAPE_CHARACTER);
        options.add(NULL_LITERAL);
        options.add(WRITE_BIGDECIMAL_IN_SCIENTIFIC_NOTATION);
        return options;
    }
}
