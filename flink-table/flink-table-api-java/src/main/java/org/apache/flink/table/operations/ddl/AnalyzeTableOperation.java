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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/** Operation to describe an {@code ANALYZE TABLE} statement. */
@Internal
public class AnalyzeTableOperation implements Operation {
    private final ObjectIdentifier tableIdentifier;
    private final @Nullable List<CatalogPartitionSpec> partitionSpecs;
    private final List<Column> columns;

    public AnalyzeTableOperation(
            ObjectIdentifier tableIdentifier,
            @Nullable List<CatalogPartitionSpec> partitionSpecs,
            List<Column> columns) {
        this.tableIdentifier = tableIdentifier;
        this.partitionSpecs = partitionSpecs;
        this.columns = Objects.requireNonNull(columns, "columns is null");
    }

    public ObjectIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    /**
     * Returns Optional.empty() if the table is not a partition table, else returns the given
     * partition specs.
     */
    public Optional<List<CatalogPartitionSpec>> getPartitionSpecs() {
        return Optional.ofNullable(partitionSpecs);
    }

    public List<Column> getColumns() {
        return columns;
    }

    @Override
    public String asSummaryString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ANALYZE TABLE ").append(tableIdentifier.toString());
        if (partitionSpecs != null) {
            sb.append(" PARTITION(")
                    .append(
                            partitionSpecs.stream()
                                    .map(p -> p.getPartitionSpec().toString())
                                    .collect(Collectors.joining(",")))
                    .append(")");
        }
        sb.append(" COMPUTE STATISTICS");
        if (!columns.isEmpty()) {
            sb.append(" FOR COLUMNS ")
                    .append(columns.stream().map(Column::getName).collect(Collectors.joining(",")));
        }

        return sb.toString();
    }
}
