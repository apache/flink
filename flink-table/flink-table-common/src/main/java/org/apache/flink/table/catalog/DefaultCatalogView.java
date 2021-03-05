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

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of a {@link CatalogView}. */
@Internal
class DefaultCatalogView implements CatalogView {

    private final Schema schema;
    private final @Nullable String comment;
    private final String originalQuery;
    private final String expandedQuery;
    private final Map<String, String> options;

    DefaultCatalogView(
            Schema schema,
            @Nullable String comment,
            String originalQuery,
            String expandedQuery,
            Map<String, String> options) {
        this.schema = checkNotNull(schema, "Schema must not be null.");
        this.comment = comment;
        this.originalQuery = checkNotNull(originalQuery, "Original query must not be null.");
        this.expandedQuery = checkNotNull(expandedQuery, "Expanded query must not be null.");
        this.options = checkNotNull(options, "Options must not be null.");

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
    public String getOriginalQuery() {
        return originalQuery;
    }

    @Override
    public String getExpandedQuery() {
        return expandedQuery;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public CatalogBaseTable copy() {
        return new DefaultCatalogView(schema, comment, originalQuery, expandedQuery, options);
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
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DefaultCatalogView that = (DefaultCatalogView) o;
        return schema.equals(that.schema)
                && Objects.equals(comment, that.comment)
                && originalQuery.equals(that.originalQuery)
                && expandedQuery.equals(that.expandedQuery)
                && options.equals(that.options);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema, comment, originalQuery, expandedQuery, options);
    }
}
