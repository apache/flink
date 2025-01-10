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
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.operations.QueryOperation;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Optional;

/** A view created from a {@link QueryOperation} via operations on {@link Table}. */
@Internal
public final class QueryOperationCatalogView implements CatalogView {

    private final QueryOperation queryOperation;
    private final @Nullable CatalogView originalView;

    public QueryOperationCatalogView(QueryOperation queryOperation) {
        this(queryOperation, null);
    }

    public QueryOperationCatalogView(
            final QueryOperation queryOperation, final CatalogView originalView) {
        this.queryOperation = queryOperation;
        this.originalView = originalView;
    }

    public QueryOperation getQueryOperation() {
        return queryOperation;
    }

    @Override
    public Schema getUnresolvedSchema() {
        return Schema.newBuilder().fromResolvedSchema(queryOperation.getResolvedSchema()).build();
    }

    @Override
    public Map<String, String> getOptions() {
        if (originalView == null) {
            throw new TableException("A view backed by a query operation has no options.");
        } else {
            return originalView.getOptions();
        }
    }

    @Override
    public String getComment() {
        return Optional.ofNullable(originalView)
                .map(CatalogView::getComment)
                .orElseGet(queryOperation::asSummaryString);
    }

    @Override
    public QueryOperationCatalogView copy() {
        return new QueryOperationCatalogView(queryOperation, originalView);
    }

    @Override
    public Optional<String> getDescription() {
        return Optional.of(getComment());
    }

    @Override
    public Optional<String> getDetailedDescription() {
        return getDescription();
    }

    @Override
    public String getOriginalQuery() {
        if (originalView == null) {
            throw new TableException(
                    "A view backed by a query operation has no serializable representation.");
        } else {
            return originalView.getOriginalQuery();
        }
    }

    @Override
    public String getExpandedQuery() {
        if (originalView == null) {
            throw new TableException(
                    "A view backed by a query operation has no serializable representation.");
        } else {
            return originalView.getExpandedQuery();
        }
    }

    @Internal
    public boolean supportsShowCreateView() {
        return originalView != null;
    }
}
