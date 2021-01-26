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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** An abstract catalog view. */
public abstract class AbstractCatalogView implements CatalogView {
    // Original text of the view definition.
    private final String originalQuery;

    // Expanded text of the original view definition
    // This is needed because the context such as current DB is
    // lost after the session, in which view is defined, is gone.
    // Expanded query text takes care of the this, as an example.
    private final String expandedQuery;

    private final TableSchema schema;
    private final Map<String, String> options;
    private final String comment;

    public AbstractCatalogView(
            String originalQuery,
            String expandedQuery,
            TableSchema schema,
            Map<String, String> options,
            String comment) {
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(originalQuery),
                "originalQuery cannot be null or empty");
        checkArgument(
                !StringUtils.isNullOrWhitespaceOnly(expandedQuery),
                "expandedQuery cannot be null or empty");

        this.originalQuery = originalQuery;
        this.expandedQuery = expandedQuery;
        this.schema = checkNotNull(schema, "schema cannot be null");
        this.options = checkNotNull(options, "options cannot be null");
        this.comment = comment;
    }

    @Override
    public String getOriginalQuery() {
        return this.originalQuery;
    }

    @Override
    public String getExpandedQuery() {
        return this.expandedQuery;
    }

    @Override
    public Map<String, String> getOptions() {
        return this.options;
    }

    @Override
    public TableSchema getSchema() {
        return this.schema;
    }

    public String getComment() {
        return this.comment;
    }
}
