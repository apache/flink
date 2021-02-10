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

package org.apache.flink.formats.json.canal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;

import java.util.Set;

/** Option utils for canal-json format. */
public class CanalJsonOptions extends JsonOptions {

    public static final ConfigOption<String> DATABASE_INCLUDE =
            ConfigOptions.key("database.include")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional regular expression to only read the specific databases changelog rows by regular matching the \"database\" meta field in the Canal record."
                                    + "The pattern string is compatible with Java's Pattern.");

    public static final ConfigOption<String> TABLE_INCLUDE =
            ConfigOptions.key("table.include")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "An optional regular expression to only read the specific tables changelog rows by regular matching the \"table\" meta field in the Canal record."
                                    + "The pattern string is compatible with Java's Pattern.");

    // ------------------------------------------------------------------------------------------
    // Canal attributes
    // ------------------------------------------------------------------------------------------

    private String databaseInclude;
    private String tableInclude;

    private CanalJsonOptions(
            boolean failOnMissingField,
            boolean ignoreParseErrors,
            MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral,
            TimestampFormat timestampFormat,
            boolean encodeDecimalAsPlainNumber,
            String databaseInclude,
            String tableInclude) {
        super(
                failOnMissingField,
                ignoreParseErrors,
                mapNullKeyMode,
                mapNullKeyLiteral,
                timestampFormat,
                encodeDecimalAsPlainNumber);
        this.databaseInclude = databaseInclude;
        this.tableInclude = tableInclude;
    }

    public String getDatabaseInclude() {
        return databaseInclude;
    }

    public String getTableInclude() {
        return tableInclude;
    }

    // ------------------------------------------------------------------------------------------
    // Builder
    // ------------------------------------------------------------------------------------------

    /** Creates A builder for building a {@link CanalJsonOptions}. */
    public static Builder builder(ReadableConfig conf) {
        return new Builder(conf);
    }

    /** A builder for creating a {@link CanalJsonOptions}. */
    @Internal
    public static class Builder extends JsonOptions.Builder {
        private String databaseInclude;
        private String tableInclude;

        public Builder(ReadableConfig conf) {
            super(conf);
            this.databaseInclude = conf.get(DATABASE_INCLUDE);
            this.tableInclude = conf.get(TABLE_INCLUDE);
        }

        public Builder setDatabaseInclude(String databaseInclude) {
            this.databaseInclude = databaseInclude;
            return this;
        }

        public Builder setTableInclude(String tableInclude) {
            this.tableInclude = tableInclude;
            return this;
        }

        public CanalJsonOptions build() {
            return new CanalJsonOptions(
                    failOnMissingField,
                    ignoreParseErrors,
                    mapNullKeyMode,
                    mapNullKeyLiteral,
                    timestampFormat,
                    encodeDecimalAsPlainNumber,
                    databaseInclude,
                    tableInclude);
        }
    }

    // --------------------------------------------------------------------------------------------
    // Validation
    // --------------------------------------------------------------------------------------------

    /** Validator for canal decoding format. */
    public static void validateDecodingFormatOptions(ReadableConfig tableOptions) {
        JsonOptions.validateDecodingFormatOptions(tableOptions);
    }

    /** Validator for canal encoding format. */
    public static void validateEncodingFormatOptions(ReadableConfig tableOptions) {
        JsonOptions.validateEncodingFormatOptions(tableOptions);
    }

    public static Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = JsonOptions.optionalOptions();
        options.add(DATABASE_INCLUDE);
        options.add(TABLE_INCLUDE);
        return options;
    }
}
