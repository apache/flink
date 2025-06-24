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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

/** Operation to alter the properties of partition. */
@Internal
public class AlterPartitionPropertiesOperation extends AlterPartitionOperation {

    private final CatalogPartition catalogPartition;

    public AlterPartitionPropertiesOperation(
            ObjectIdentifier tableIdentifier,
            CatalogPartitionSpec partitionSpec,
            CatalogPartition catalogPartition) {
        super(tableIdentifier, partitionSpec);
        this.catalogPartition = catalogPartition;
    }

    public CatalogPartition getCatalogPartition() {
        return catalogPartition;
    }

    @Override
    public String asSummaryString() {
        String spec = OperationUtils.formatPartitionSpec(partitionSpec);
        String properties = OperationUtils.formatProperties(catalogPartition.getProperties());
        return String.format(
                "ALTER TABLE %s PARTITION (%s) SET (%s)",
                tableIdentifier.asSummaryString(), spec, properties);
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        Catalog catalog =
                ctx.getCatalogManager()
                        .getCatalogOrThrowException(getTableIdentifier().getCatalogName());
        try {
            catalog.alterPartition(
                    getTableIdentifier().toObjectPath(),
                    getPartitionSpec(),
                    getCatalogPartition(),
                    ignoreIfTableNotExists());
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (Exception e) {
            throw new TableException(String.format("Could not execute %s", asSummaryString()), e);
        }
    }
}
