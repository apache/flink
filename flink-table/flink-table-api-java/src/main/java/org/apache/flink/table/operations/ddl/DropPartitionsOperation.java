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
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.operations.OperationUtils;

import java.util.List;

/** Operation to describe ALTER TABLE DROP PARTITION statement. */
@Internal
public class DropPartitionsOperation extends AlterTableOperation {

    private final boolean ignoreIfPartitionNotExists;
    private final List<CatalogPartitionSpec> partitionSpecs;

    public DropPartitionsOperation(
            ObjectIdentifier tableIdentifier,
            boolean ignoreIfPartitionNotExists,
            List<CatalogPartitionSpec> partitionSpecs) {
        super(tableIdentifier, false);
        this.ignoreIfPartitionNotExists = ignoreIfPartitionNotExists;
        this.partitionSpecs = partitionSpecs;
    }

    public boolean ignoreIfPartitionNotExists() {
        return ignoreIfPartitionNotExists;
    }

    public List<CatalogPartitionSpec> getPartitionSpecs() {
        return partitionSpecs;
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder =
                new StringBuilder(
                        String.format("ALTER TABLE %s DROP", tableIdentifier.asSummaryString()));
        if (ignoreIfPartitionNotExists) {
            builder.append(" IF EXISTS");
        }
        for (CatalogPartitionSpec spec : partitionSpecs) {
            builder.append(
                    String.format(" PARTITION (%s)", OperationUtils.formatPartitionSpec(spec)));
        }
        return builder.toString();
    }

    @Override
    public TableResultInternal execute(Context ctx) {
        ObjectPath tablePath = getTableIdentifier().toObjectPath();
        Catalog catalog =
                ctx.getCatalogManager()
                        .getCatalogOrThrowException(getTableIdentifier().getCatalogName());
        try {
            for (CatalogPartitionSpec spec : getPartitionSpecs()) {
                catalog.dropPartition(tablePath, spec, ignoreIfPartitionNotExists());
            }
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (Exception e) {
            throw new TableException(String.format("Could not execute %s", asSummaryString()), e);
        }
    }
}
