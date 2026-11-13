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
import org.apache.flink.table.catalog.IntervalFreshness;
import org.apache.flink.table.catalog.TableDistribution;
import org.apache.flink.table.utils.EncodingUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Describes a materialized table for {@link Table#materializeInto(String,
 * MaterializedTableDescriptor)}.
 *
 * <p>A descriptor is a reusable value, it can be shared between tables and copied with {@link
 * #toBuilder()}.
 *
 * <p>A declared {@link Schema} may override query column types with implicitly castable ones, and
 * add a primary key, a watermark, computed columns and virtual metadata columns. Every declared
 * persisted (physical or non-virtual metadata) column must be produced by the query.
 *
 * <pre>{@code
 * table.materializeInto(
 *         "my_mt",
 *         MaterializedTableDescriptor.newBuilder()
 *             .freshness(Duration.ofMinutes(1))
 *             .continuousRefresh()
 *             .build())
 *     .execute();
 * }</pre>
 */
@PublicEvolving
public class MaterializedTableDescriptor {

    private final @Nullable Schema schema;
    private final Map<String, String> options;
    private final @Nullable TableDistribution distribution;
    private final List<String> partitionKeys;
    private final @Nullable String comment;
    private final @Nullable IntervalFreshness freshness;
    private final RefreshStrategy refreshStrategy;

    protected MaterializedTableDescriptor(MaterializedTableDescriptor descriptor) {
        this.schema = descriptor.schema;
        this.options = descriptor.options;
        this.distribution = descriptor.distribution;
        this.partitionKeys = descriptor.partitionKeys;
        this.comment = descriptor.comment;
        this.freshness = descriptor.freshness;
        this.refreshStrategy = descriptor.refreshStrategy;
    }

    private MaterializedTableDescriptor(Builder builder) {
        this.schema = builder.schema;
        this.options = Collections.unmodifiableMap(new LinkedHashMap<>(builder.options));
        this.distribution = builder.distribution;
        this.partitionKeys = List.copyOf(builder.partitionKeys);
        this.comment = builder.comment;
        this.freshness = builder.freshness;
        this.refreshStrategy = builder.refreshStrategy;
    }

    /** Creates a builder for a declaration that uses all defaults. */
    public static Builder newBuilder() {
        return new Builder();
    }

    /** Creates a builder initialized with this declaration. */
    public Builder toBuilder() {
        return new Builder(this);
    }

    public Optional<Schema> getSchema() {
        return Optional.ofNullable(schema);
    }

    public Map<String, String> getOptions() {
        return options;
    }

    public Optional<TableDistribution> getDistribution() {
        return Optional.ofNullable(distribution);
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public Optional<String> getComment() {
        return Optional.ofNullable(comment);
    }

    public Optional<IntervalFreshness> getFreshness() {
        return Optional.ofNullable(freshness);
    }

    /** Defaults to {@link RefreshStrategy#automatic()}. */
    public RefreshStrategy getRefreshStrategy() {
        return refreshStrategy;
    }

    @Override
    public String toString() {
        final List<String> clauses = new ArrayList<>();
        if (schema != null) {
            clauses.add(schema.toString());
        }
        if (comment != null) {
            clauses.add("COMMENT '" + EncodingUtils.escapeSingleQuotes(comment) + "'");
        }
        if (distribution != null) {
            clauses.add(distribution.toString());
        }
        if (!partitionKeys.isEmpty()) {
            clauses.add(
                    partitionKeys.stream()
                            .map(EncodingUtils::escapeIdentifier)
                            .collect(Collectors.joining(", ", "PARTITIONED BY (", ")")));
        }
        if (!options.isEmpty()) {
            clauses.add(
                    options.entrySet().stream()
                            .map(
                                    entry ->
                                            String.format(
                                                    "  '%s' = '%s'",
                                                    EncodingUtils.escapeSingleQuotes(
                                                            entry.getKey()),
                                                    EncodingUtils.escapeSingleQuotes(
                                                            entry.getValue())))
                            .collect(
                                    Collectors.joining(
                                            String.format(",%n"),
                                            String.format("WITH (%n"),
                                            String.format("%n)"))));
        }
        if (freshness != null) {
            clauses.add("FRESHNESS = " + freshness);
        }
        if (refreshStrategy.getKind() != RefreshStrategy.Kind.AUTOMATIC) {
            clauses.add("REFRESH_MODE = " + refreshStrategy.asSummaryString());
        }
        return String.join(String.format("%n"), clauses);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final MaterializedTableDescriptor that = (MaterializedTableDescriptor) o;
        return Objects.equals(schema, that.schema)
                && options.equals(that.options)
                && Objects.equals(distribution, that.distribution)
                && partitionKeys.equals(that.partitionKeys)
                && Objects.equals(comment, that.comment)
                && Objects.equals(freshness, that.freshness)
                && refreshStrategy.equals(that.refreshStrategy);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                schema, options, distribution, partitionKeys, comment, freshness, refreshStrategy);
    }

    // ---------------------------------------------------------------------------------------------

    /** Builder for {@link MaterializedTableDescriptor}. */
    @PublicEvolving
    public static class Builder {

        private @Nullable Schema schema;
        private final Map<String, String> options;
        private @Nullable TableDistribution distribution;
        private final List<String> partitionKeys;
        private @Nullable String comment;
        private @Nullable IntervalFreshness freshness;
        private RefreshStrategy refreshStrategy = RefreshStrategy.automatic();

        protected Builder() {
            this.options = new LinkedHashMap<>();
            this.partitionKeys = new ArrayList<>();
        }

        protected Builder(MaterializedTableDescriptor descriptor) {
            this.schema = descriptor.schema;
            this.options = new LinkedHashMap<>(descriptor.options);
            this.distribution = descriptor.distribution;
            this.partitionKeys = new ArrayList<>(descriptor.partitionKeys);
            this.comment = descriptor.comment;
            this.freshness = descriptor.freshness;
            this.refreshStrategy = descriptor.refreshStrategy;
        }

        /**
         * Declares the schema. It is merged onto the schema of the query. {@code null} derives the
         * schema from the query.
         */
        public Builder schema(@Nullable Schema schema) {
            this.schema = schema;
            return this;
        }

        /** Sets the given option on the materialized table. */
        public <T> Builder option(ConfigOption<T> configOption, T value) {
            Preconditions.checkNotNull(configOption, "Config option must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            options.put(configOption.key(), ConfigurationUtils.convertValue(value, String.class));
            return this;
        }

        /** Sets the given option on the materialized table. Keys must be fully specified. */
        public Builder option(String key, String value) {
            Preconditions.checkNotNull(key, "Key must not be null.");
            Preconditions.checkNotNull(value, "Value must not be null.");
            options.put(key, value);
            return this;
        }

        /**
         * Distributes into buckets over the given columns. Algorithm and count are
         * connector-defined.
         */
        public Builder distributedBy(String... bucketKeys) {
            TableDescriptor.Builder.validateBucketKeys(bucketKeys);
            this.distribution = TableDistribution.ofUnknown(Arrays.asList(bucketKeys), null);
            return this;
        }

        /** Distributes into the given number of buckets over the given columns. */
        public Builder distributedBy(int numberOfBuckets, String... bucketKeys) {
            TableDescriptor.Builder.validateBucketKeys(bucketKeys);
            this.distribution =
                    TableDistribution.ofUnknown(Arrays.asList(bucketKeys), numberOfBuckets);
            return this;
        }

        /** Distributes by hash over the given columns. The bucket count is connector-defined. */
        public Builder distributedByHash(String... bucketKeys) {
            TableDescriptor.Builder.validateBucketKeys(bucketKeys);
            this.distribution = TableDistribution.ofHash(Arrays.asList(bucketKeys), null);
            return this;
        }

        /** Distributes into the given number of buckets by hash over the given columns. */
        public Builder distributedByHash(int numberOfBuckets, String... bucketKeys) {
            TableDescriptor.Builder.validateBucketKeys(bucketKeys);
            this.distribution =
                    TableDistribution.ofHash(Arrays.asList(bucketKeys), numberOfBuckets);
            return this;
        }

        /** Distributes by range over the given columns. The bucket count is connector-defined. */
        public Builder distributedByRange(String... bucketKeys) {
            TableDescriptor.Builder.validateBucketKeys(bucketKeys);
            this.distribution = TableDistribution.ofRange(Arrays.asList(bucketKeys), null);
            return this;
        }

        /** Distributes into the given number of buckets by range over the given columns. */
        public Builder distributedByRange(int numberOfBuckets, String... bucketKeys) {
            TableDescriptor.Builder.validateBucketKeys(bucketKeys);
            this.distribution =
                    TableDistribution.ofRange(Arrays.asList(bucketKeys), numberOfBuckets);
            return this;
        }

        /** Distributes into the given number of buckets. The algorithm is connector-defined. */
        public Builder distributedInto(int numberOfBuckets) {
            this.distribution = TableDistribution.ofUnknown(numberOfBuckets);
            return this;
        }

        /**
         * Adds the given partition keys. They are only applied when the materialized table is
         * created, not when an existing one is altered.
         */
        public Builder partitionedBy(String... partitionKeys) {
            this.partitionKeys.addAll(Arrays.asList(partitionKeys));
            return this;
        }

        /**
         * Declares the comment; {@code null} clears it. It is only applied when the materialized
         * table is created, not when an existing one is altered.
         */
        public Builder comment(@Nullable String comment) {
            this.comment = comment;
            return this;
        }

        /**
         * Declares the freshness, i.e. how stale the materialized data may become. {@code
         * (IntervalFreshness) null} clears it so the engine applies its default. It is only applied
         * when the materialized table is created, not when an existing one is altered.
         */
        public Builder freshness(@Nullable IntervalFreshness freshness) {
            this.freshness = freshness;
            return this;
        }

        /**
         * Declares the freshness as a positive, whole number of seconds (at most {@link
         * Integer#MAX_VALUE}). It is stored in its largest whole unit, e.g. 120 seconds becomes
         * {@code INTERVAL '2' MINUTE}. It is only applied when the materialized table is created,
         * not when an existing one is altered.
         *
         * @throws ValidationException if the freshness is not positive, has a sub-second part, or
         *     exceeds {@link Integer#MAX_VALUE} seconds
         */
        public Builder freshness(Duration freshness) {
            Preconditions.checkNotNull(freshness, "Freshness must not be null.");
            if (freshness.isNegative() || freshness.isZero()) {
                throw new ValidationException(
                        String.format("Freshness must be positive, but was: %s", freshness));
            }

            if (freshness.getNano() != 0) {
                throw new ValidationException(
                        String.format(
                                "Freshness must be a whole number of seconds, but was: %s",
                                freshness));
            }
            if (freshness.getSeconds() > Integer.MAX_VALUE) {
                throw new ValidationException(
                        String.format(
                                "Freshness must be at most %d seconds, but was: %s",
                                Integer.MAX_VALUE, freshness));
            }
            return freshness(IntervalFreshness.fromDuration(freshness));
        }

        /** Declares the refresh strategy. Defaults to {@link RefreshStrategy#automatic()}. */
        public Builder refreshStrategy(RefreshStrategy refreshStrategy) {
            this.refreshStrategy =
                    Preconditions.checkNotNull(
                            refreshStrategy, "Refresh strategy must not be null.");
            return this;
        }

        /** Shorthand for {@code refreshStrategy(RefreshStrategy.continuous())}. */
        public Builder continuousRefresh() {
            return refreshStrategy(RefreshStrategy.continuous());
        }

        /** Shorthand for {@code refreshStrategy(RefreshStrategy.full())}. */
        public Builder fullRefresh() {
            return refreshStrategy(RefreshStrategy.full());
        }

        /** Returns an immutable {@link MaterializedTableDescriptor}. */
        public MaterializedTableDescriptor build() {
            return new MaterializedTableDescriptor(this);
        }
    }
}
