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

import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Operation to describe a DESCRIBE [EXTENDED] [[catalogName.] dataBasesName].sqlIdentifier
 * statement.
 */
public class DescribeTableOperation implements Operation {

    private final ObjectIdentifier sqlIdentifier;
    private final boolean isExtended;
    private final String columnName;
    private final @Nullable List<CatalogPartitionSpec> partitionSpecs;

    public DescribeTableOperation(
            ObjectIdentifier sqlIdentifier,
            boolean isExtended,
            String columnName,
            @Nullable List<CatalogPartitionSpec> partitionSpecs) {
        this.sqlIdentifier = sqlIdentifier;
        this.isExtended = isExtended;
        this.columnName = columnName;
        this.partitionSpecs = partitionSpecs;
    }

    public ObjectIdentifier getSqlIdentifier() {
        return sqlIdentifier;
    }

    public boolean isExtended() {
        return isExtended;
    }

    public Optional<String> getColumnName() {
        return Optional.ofNullable(columnName);
    }

    /**
     * Returns Optional.empty() if the table is not a partition table, else returns the given
     * partition specs.
     */
    public Optional<List<CatalogPartitionSpec>> getPartitionSpecs() {
        return Optional.ofNullable(partitionSpecs);
    }

    @Override
    public String asSummaryString() {
        StringBuilder sb = new StringBuilder();
        sb.append("DESCRIBE");
        if (isExtended) {
            sb.append(" EXTENDED");
        }
        sb.append(sqlIdentifier.toString());
        if (partitionSpecs != null) {
            sb.append(" PARTITION(")
                    .append(
                            partitionSpecs.stream()
                                    .map(p -> p.getPartitionSpec().toString())
                                    .collect(Collectors.joining(",")))
                    .append(")");
        }
        if (columnName.length() != 0) {
            sb.append(String.format(" %s", columnName));
        }
        return sb.toString();
    }
}
