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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.ValidationException;

import java.util.Set;

import static org.apache.flink.formats.json.debezium.DebeziumJsonFormatFactory.IDENTIFIER;

/** Option utils for debezium-json format. */
public class DebeziumJsonOptions extends JsonOptions {

    public static final ConfigOption<Boolean> SCHEMA_INCLUDE =
            ConfigOptions.key("schema-include")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "When setting up a Debezium Kafka Connect, users can enable "
                                    + "a Kafka configuration 'value.converter.schemas.enable' to include schema in the message. "
                                    + "This option indicates the Debezium JSON data include the schema in the message or not. "
                                    + "Default is false.");

    // ------------------------------------------------------------------------------------------
    // Debezium attributes
    // ------------------------------------------------------------------------------------------

    private boolean schemaInclude;

    private DebeziumJsonOptions(
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            TimestampFormat timestampFormat,
            boolean encodeDecimalAsPlainNumber,
            boolean schemaInclude) {
        super(
                failOnMissingField,
                ignoreParseErrors,
                mapNullKeyMode,
                mapNullKeyLiteral,
                timestampFormat,
                encodeDecimalAsPlainNumber);
        this.schemaInclude = schemaInclude;
    }

    public boolean isSchemaInclude() {
        return schemaInclude;
    }

    // ------------------------------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------------------------------

    /** Creates A builder for building a {@link DebeziumJsonOptions}. */
    public static Builder builder(ReadableConfig conf) {
        return new Builder(conf);
    }

    /** A builder for creating a {@link DebeziumJsonOptions}. */
    @Internal
    public static class Builder extends JsonOptions.Builder {
        private boolean schemaInclude;

        public Builder(ReadableConfig conf) {
            super(conf);
            this.schemaInclude = conf.get(SCHEMA_INCLUDE);
        }

        public Builder setSchemaInclude(boolean schemaInclude) {
            this.schemaInclude = schemaInclude;
            return this;
        }

        public DebeziumJsonOptions build() {
            return new DebeziumJsonOptions(
                    failOnMissingField,
                    ignoreParseErrors,
                    mapNullKeyMode,
                    mapNullKeyLiteral,
                    timestampFormat,
                    encodeDecimalAsPlainNumber,
                    schemaInclude);
        }
    }

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

    public static Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = JsonOptions.optionalOptions();
        options.add(SCHEMA_INCLUDE);
        return options;
    }
}
