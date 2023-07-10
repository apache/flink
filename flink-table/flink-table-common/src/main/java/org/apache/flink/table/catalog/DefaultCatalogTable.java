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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.Schema;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of a {@link CatalogTable}. */
@Internal
public class DefaultCatalogTable implements CatalogTable {

    private final Schema schema;
    private final @Nullable String comment;
    private final List<String> partitionKeys;
    private final Map<String, String> options;

    private final @Nullable Long snapshot;

    protected DefaultCatalogTable(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options) {
        this(schema, comment, partitionKeys, options, null);
    }

    protected DefaultCatalogTable(
            Schema schema,
            @Nullable String comment,
            List<String> partitionKeys,
            Map<String, String> options,
            @Nullable Long snapshot) {
        this.schema = checkNotNull(schema, "Schema must not be null.");
        this.comment = comment;
        this.partitionKeys = checkNotNull(partitionKeys, "Partition keys must not be null.");
        this.options = checkNotNull(options, "Options must not be null.");
        this.snapshot = snapshot;

        checkArgument(
                options.entrySet().stream()
                        .allMatch(e -> e.getKey() != null && e.getValue() != null),
                "Options cannot have null keys or values.");
    }

    @Override
    public Schema getUnresolvedSchema() {
        return schema;
    }

    @Override
    public String getComment() {
        return comment != null ? comment : "";
    }

    @Override
    public boolean isPartitioned() {
        return !partitionKeys.isEmpty();
    }

    @Override
    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public CatalogBaseTable copy() {
        return new DefaultCatalogTable(schema, comment, partitionKeys, options, snapshot);
    }

    @Override
    public CatalogTable copy(Map<String, String> options) {
        return new DefaultCatalogTable(schema, comment, partitionKeys, options, snapshot);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return Optional.empty();
    }

    @Override
    public Map<String, String> toProperties() {
        throw new UnsupportedOperationException(
                "Only a resolved catalog table can be serialized into a map of string properties.");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultCatalogTable that = (DefaultCatalogTable) o;
        return schema.equals(that.schema)
                && Objects.equals(comment, that.comment)
                && partitionKeys.equals(that.partitionKeys)
                && options.equals(that.options)
                && Objects.equals(snapshot, that.snapshot);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, comment, partitionKeys, options, snapshot);
    }

    @Override
    public String toString() {
        return "DefaultCatalogTable{"
                + "schema="
                + schema
                + ", comment='"
                + comment
                + '\''
                + ", partitionKeys="
                + partitionKeys
                + ", options="
                + options
                + ", snapshot="
                + snapshot
                + '}';
    }

    @Override
    public Optional<Long> getSnapshot() {
        return Optional.ofNullable(snapshot);
    }
}
