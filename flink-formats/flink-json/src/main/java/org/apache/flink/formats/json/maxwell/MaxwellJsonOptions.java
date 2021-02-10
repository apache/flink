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

package org.apache.flink.formats.json.maxwell;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;

import java.util.Set;

/** Option utils for maxwell-json format. */
public class MaxwellJsonOptions extends JsonOptions {

    // ------------------------------------------------------------------------------------------
    // Maxwell attributes
    // ------------------------------------------------------------------------------------------

    private MaxwellJsonOptions(
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            TimestampFormat timestampFormat,
            boolean encodeDecimalAsPlainNumber) {
        super(
                failOnMissingField,
                ignoreParseErrors,
                mapNullKeyMode,
                mapNullKeyLiteral,
                timestampFormat,
                encodeDecimalAsPlainNumber);
    }

    // ------------------------------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------------------------------

    /** Creates A builder for building a {@link MaxwellJsonOptions}. */
    public static Builder builder(ReadableConfig conf) {
        return new Builder(conf);
    }

    /** A builder for creating a {@link MaxwellJsonOptions}. */
    @Internal
    public static class Builder extends JsonOptions.Builder {

        public Builder(ReadableConfig conf) {
            super(conf);
        }

        public MaxwellJsonOptions build() {
            return new MaxwellJsonOptions(
                    failOnMissingField,
                    ignoreParseErrors,
                    mapNullKeyMode,
                    mapNullKeyLiteral,
                    timestampFormat,
                    encodeDecimalAsPlainNumber);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    /** Validator for maxwell decoding format. */
    public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
        JsonOptions.validateDecodingFormatOptions(tableOptions);
    }

    /** Validator for maxwell encoding format. */
    public static void validateEncodingFormatOptions(ReadableConfig tableOptions) {
        JsonOptions.validateEncodingFormatOptions(tableOptions);
    }

    public static Set<ConfigOption<?>> optionalOptions() {
        return JsonOptions.optionalOptions();
    }
}
