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

import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

import java.util.List;
import java.util.Map;

/** Operation to describe ALTER TABLE ADD PARTITION statement. */
public class AddPartitionsOperation extends AlterTableOperation {

    private final boolean ifNotExists;
    private final List<CatalogPartitionSpec> partitionSpecs;
    private final List<CatalogPartition> catalogPartitions;

    public AddPartitionsOperation(
            ObjectIdentifier tableIdentifier,
            boolean ifNotExists,
            List<CatalogPartitionSpec> partitionSpecs,
            List<CatalogPartition> catalogPartitions) {
        super(tableIdentifier);
        this.ifNotExists = ifNotExists;
        this.partitionSpecs = partitionSpecs;
        this.catalogPartitions = catalogPartitions;
    }

    public List<CatalogPartitionSpec> getPartitionSpecs() {
        return partitionSpecs;
    }

    public List<CatalogPartition> getCatalogPartitions() {
        return catalogPartitions;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder =
                new StringBuilder(
                        String.format("ALTER TABLE %s ADD", tableIdentifier.asSummaryString()));
        if (ifNotExists) {
            builder.append(" IF NOT EXISTS");
        }
        for (int i = 0; i < partitionSpecs.size(); i++) {
            String spec = OperationUtils.formatPartitionSpec(partitionSpecs.get(i));
            builder.append(String.format(" PARTITION (%s)", spec));
            Map<String, String> properties = catalogPartitions.get(i).getProperties();
            if (!properties.isEmpty()) {
                builder.append(
                        String.format(" WITH (%s)", OperationUtils.formatProperties(properties)));
            }
        }
        return builder.toString();
    }
}
