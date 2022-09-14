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

/** Operation to describe a SHOW PARTITIONS statement. */
public class ShowPartitionsOperation implements ShowOperation {

    protected final ObjectIdentifier tableIdentifier;
    private final CatalogPartitionSpec partitionSpec;
    // the name for the default partition, which usually means the partition's value is null or
    // empty string
    @Nullable private final String defaultPartitionName;

    public ShowPartitionsOperation(
            ObjectIdentifier tableIdentifier, CatalogPartitionSpec partitionSpec) {
        this(tableIdentifier, partitionSpec, null);
    }

    public ShowPartitionsOperation(
            ObjectIdentifier tableIdentifier,
            CatalogPartitionSpec partitionSpec,
            @Nullable String defaultPartitionName) {
        this.tableIdentifier = tableIdentifier;
        this.partitionSpec = partitionSpec;
        this.defaultPartitionName = defaultPartitionName;
    }

    public ObjectIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public CatalogPartitionSpec getPartitionSpec() {
        return partitionSpec;
    }

    public String getDefaultPartitionName() {
        return defaultPartitionName;
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder =
                new StringBuilder(
                        String.format("SHOW PARTITIONS %s", tableIdentifier.asSummaryString()));
        if (partitionSpec != null) {
            builder.append(
                    String.format(
                            " PARTITION (%s)", OperationUtils.formatPartitionSpec(partitionSpec)));
        }
        return builder.toString();
    }
}
