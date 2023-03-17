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

package org.apache.flink.table.operations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ContextResolvedTable;
import org.apache.flink.table.connector.sink.abilities.SupportsDeletePushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** The operation for deleting data in a table according to filters directly. */
@Internal
public class DeleteFromFilterOperation extends SinkModifyOperation {

    @Nonnull private final SupportsDeletePushDown supportsDeletePushDownSink;
    @Nonnull private final List<ResolvedExpression> filters;

    public DeleteFromFilterOperation(
            ContextResolvedTable contextResolvedTable,
            @Nonnull SupportsDeletePushDown supportsDeletePushDownSink,
            @Nonnull List<ResolvedExpression> filters) {
        super(contextResolvedTable, null, null, ModifyType.DELETE);
        this.supportsDeletePushDownSink = Preconditions.checkNotNull(supportsDeletePushDownSink);
        this.filters = Preconditions.checkNotNull(filters);
    }

    @Nonnull
    public SupportsDeletePushDown getSupportsDeletePushDownSink() {
        return supportsDeletePushDownSink;
    }

    @Nonnull
    public List<ResolvedExpression> getFilters() {
        return filters;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", getContextResolvedTable().getIdentifier().asSummaryString());
        params.put("filters", filters);
        return OperationUtils.formatWithChildren(
                "DeleteFromFilter", params, Collections.emptyList(), Operation::asSummaryString);
    }

    @Override
    public QueryOperation getChild() {
        throw new UnsupportedOperationException("This shouldn't be called");
    }

    @Override
    public <T> T accept(ModifyOperationVisitor<T> visitor) {
        throw new UnsupportedOperationException("This shouldn't be called");
    }
}
