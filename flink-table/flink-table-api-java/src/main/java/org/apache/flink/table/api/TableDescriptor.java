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

package org.apache.flink.table.api;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.format.Format;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Describes a {@link CatalogTable} representing a source or sink.
 *
 * <p>A {@link TableDescriptor} is a template for creating a {@link CatalogTable} instance. It
 * closely resembles the "CREATE TABLE" SQL DDL statement, containing schema, connector options, and
 * other characteristics. Since tables in Flink are typically backed by external systems, the
 * descriptor describes how a connector (and possibly its format) are configured.
 *
 * <p>This can be used to register a table in the Table API, see {@link
 * TableEnvironment#createTemporaryTable(String, TableDescriptor)}.
 */
@PublicEvolving
public class TableDescriptor {

    private final @Nullable Schema schema;
    private final Map<String, String> options;
    private final List<String> partitionKeys;
    private final @Nullable String comment;

    protected TableDescriptor(
            @Nullable Schema schema,
            Map<String, String> options,
            List<String> partitionKeys,
            @Nullable String comment) {
        this.schema = schema;
        this.options = Collections.unmodifiableMap(options);
        this.partitionKeys = Collections.unmodifiableList(partitionKeys);
        this.comment = comment;
    }

    /**
     * Creates a new {@link Builder} for a table using the given connector.
     *
     * @param connector The factory identifier for the connector.
     */
    public static Builder forConnector(String connector) {
        Preconditions.checkNotNull(connector, "Table descriptors require a connector identifier.");
        final Builder descriptorBuilder = new Builder();
        descriptorBuilder.option(FactoryUtil.CONNECTOR, connector);
        return descriptorBuilder;
    }

    /**
     * Creates a new {@link Builder} for a managed table.
     *
     * @deprecated This method will be removed soon. Please see FLIP-346 for more details.
     */
    @Deprecated
    public static Builder forManaged() {
        return new Builder();
    }

    // ---------------------------------------------------------------------------------------------

    public Optional<Schema> getSchema() {
        return Optional.ofNullable(schema);
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    // ---------------------------------------------------------------------------------------------

    /** Converts this descriptor into a {@link CatalogTable}. */
    public CatalogTable toCatalogTable() {
        final Schema schema =
                getSchema()
                        .orElseThrow(
                                () ->
                                        new ValidationException(
                                                "Missing schema in TableDescriptor. "
                                                        + "A schema is typically required. "
                                                        + "It can only be omitted at certain "
                                                        + "documented locations."));

        return CatalogTable.of(schema, getComment().orElse(null), getPartitionKeys(), getOptions());
    }

    /** Converts this immutable instance into a mutable {@link Builder}. */
    public Builder toBuilder() {
        return new Builder(this);
    }

    // ---------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        final String escapedPartitionKeys =
                partitionKeys.stream()
                        .map(EncodingUtils::escapeIdentifier)
                        .collect(Collectors.joining(", "));

        final String partitionedBy =
                !partitionKeys.isEmpty()
                        ? String.format("PARTITIONED BY (%s)", escapedPartitionKeys)
                        : "";

        final String serializedOptions =
                options.entrySet().stream()
                        .map(
                                entry ->
                                        String.format(
                                                "  '%s' = '%s'",
                                                EncodingUtils.escapeSingleQuotes(entry.getKey()),
                                                EncodingUtils.escapeSingleQuotes(entry.getValue())))
                        .collect(Collectors.joining(String.format(",%n")));

        return String.format(
                "%s%nCOMMENT '%s'%n%s%nWITH (%n%s%n)",
                schema != null ? schema : "",
                comment != null ? comment : "",
                partitionedBy,
                serializedOptions);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        TableDescriptor that = (TableDescriptor) obj;
        return Objects.equals(schema, that.schema)
                && options.equals(that.options)
                && partitionKeys.equals(that.partitionKeys)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, options, partitionKeys, comment);
    }

    // ---------------------------------------------------------------------------------------------

    /** Builder for {@link TableDescriptor}. */
    @PublicEvolving
    public static class Builder {

        private @Nullable Schema schema;
        private final Map<String, String> options;
        private final List<String> partitionKeys;
        private @Nullable String comment;

        protected Builder() {
            this.options = new HashMap<>();
            this.partitionKeys = new ArrayList<>();
        }

        protected Builder(TableDescriptor descriptor) {
            this.schema = descriptor.getSchema().orElse(null);
            this.options = new HashMap<>(descriptor.getOptions());
            this.partitionKeys = new ArrayList<>(descriptor.getPartitionKeys());
            this.comment = descriptor.getComment().orElse(null);
        }

        /**
         * Define the schema of the {@link TableDescriptor}.
         *
         * <p>The schema is typically required. It is optional only in cases where the schema can be
         * inferred, e.g. {@link Table#insertInto(TableDescriptor)}.
         */
        public Builder schema(@Nullable Schema schema) {
            this.schema = schema;
            return this;
        }

        /** Sets the given option on the table. */
        public <T> Builder option(ConfigOption<T> configOption, T value) {
            Preconditions.checkNotNull(configOption, "Config option must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            options.put(configOption.key(), ConfigurationUtils.convertValue(value, String.class));
            return this;
        }

        /**
         * Sets the given option on the table.
         *
         * <p>Option keys must be fully specified. When defining options for a {@link Format
         * format}, use {@link #format(FormatDescriptor)} instead.
         *
         * <p>Example:
         *
         * <pre>{@code
         * TableDescriptor.forConnector("kafka")
         *   .option("scan.startup.mode", "latest-offset")
         *   .build();
         * }</pre>
         */
        public Builder option(String key, String value) {
            Preconditions.checkNotNull(key, "Key must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            options.put(key, value);
            return this;
        }

        /**
         * Defines the {@link Format format} to be used for this table.
         *
         * <p>Note that not every connector requires a format to be specified, while others may use
         * multiple formats. In the latter case, use {@link #format(ConfigOption, FormatDescriptor)}
         * instead to specify for which option the format should be configured.
         */
        public Builder format(String format) {
            return format(FactoryUtil.FORMAT, FormatDescriptor.forFormat(format).build());
        }

        /**
         * Defines the format to be used for this table.
         *
         * <p>Note that not every connector requires a format to be specified, while others may use
         * multiple formats.
         *
         * <p>Options of the provided {@param formatDescriptor} are automatically prefixed. For
         * example,
         *
         * <pre>{@code
         * descriptorBuilder.format(FormatDescriptor.forFormat("json")
         *   .option(JsonOptions.IGNORE_PARSE_ERRORS, true)
         *   .build()
         * }</pre>
         *
         * <p>will result in the options
         *
         * <pre>{@code
         * 'format' = 'json'
         * 'json.ignore-parse-errors' = 'true'
         * }</pre>
         */
        public Builder format(FormatDescriptor formatDescriptor) {
            return format(FactoryUtil.FORMAT, formatDescriptor);
        }

        /**
         * Defines the format to be used for this table.
         *
         * <p>Note that not every connector requires a format to be specified, while others may use
         * multiple formats.
         *
         * <p>Options of the provided {@param formatDescriptor} are automatically prefixed. For
         * example,
         *
         * <pre>{@code
         * descriptorBuilder.format(KafkaOptions.KEY_FORMAT, FormatDescriptor.forFormat("json")
         *   .option(JsonOptions.IGNORE_PARSE_ERRORS, true)
         *   .build()
         * }</pre>
         *
         * <p>will result in the options
         *
         * <pre>{@code
         * 'key.format' = 'json'
         * 'key.json.ignore-parse-errors' = 'true'
         * }</pre>
         */
        public Builder format(
                ConfigOption<String> formatOption, FormatDescriptor formatDescriptor) {
            Preconditions.checkNotNull(formatOption, "Format option must not be null.");
            Preconditions.checkNotNull(formatDescriptor, "Format descriptor must not be null.");

            option(formatOption, formatDescriptor.getFormat());

            final String optionPrefix =
                    FactoryUtil.getFormatPrefix(formatOption, formatDescriptor.getFormat());
            formatDescriptor
                    .getOptions()
                    .forEach(
                            (key, value) -> {
                                if (key.startsWith(optionPrefix)) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Format options set using #format(FormatDescriptor) should not contain the prefix '%s', but found '%s'.",
                                                    optionPrefix, key));
                                }

                                final String prefixedKey = optionPrefix + key;
                                option(prefixedKey, value);
                            });

            return this;
        }

        /** Define which columns this table is partitioned by. */
        public Builder partitionedBy(String... partitionKeys) {
            this.partitionKeys.addAll(Arrays.asList(partitionKeys));
            return this;
        }

        /** Define the comment for this table. */
        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        /** Returns an immutable instance of {@link TableDescriptor}. */
        public TableDescriptor build() {
            return new TableDescriptor(schema, options, partitionKeys, comment);
        }
    }
}
