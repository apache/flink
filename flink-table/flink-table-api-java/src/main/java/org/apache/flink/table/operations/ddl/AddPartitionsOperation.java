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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogPartition;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.operations.OperationUtils;

import java.util.List;
import java.util.Map;

/** Operation to describe ALTER TABLE ADD PARTITION statement. */
@Internal
public class AddPartitionsOperation extends AlterTableOperation {

    private final boolean ignoreIfPartitionExists;
    private final List<CatalogPartitionSpec> partitionSpecs;
    private final List<CatalogPartition> catalogPartitions;

    public AddPartitionsOperation(
            ObjectIdentifier tableIdentifier,
            boolean ignoreIfPartitionExists,
            List<CatalogPartitionSpec> partitionSpecs,
            List<CatalogPartition> catalogPartitions) {
        super(tableIdentifier, false);
        this.ignoreIfPartitionExists = ignoreIfPartitionExists;
        this.partitionSpecs = partitionSpecs;
        this.catalogPartitions = catalogPartitions;
    }

    public List<CatalogPartitionSpec> getPartitionSpecs() {
        return partitionSpecs;
    }

    public List<CatalogPartition> getCatalogPartitions() {
        return catalogPartitions;
    }

    public boolean ignoreIfPartitionExists() {
        return ignoreIfPartitionExists;
    }

    @Override
    public String asSummaryString() {
        StringBuilder builder =
                new StringBuilder(
                        String.format("ALTER TABLE %s ADD", tableIdentifier.asSummaryString()));
        if (ignoreIfPartitionExists) {
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

    @Override
    public TableResultInternal execute(Context ctx) {
        List<CatalogPartitionSpec> specs = getPartitionSpecs();
        List<CatalogPartition> partitions = getCatalogPartitions();
        ObjectPath tablePath = getTableIdentifier().toObjectPath();
        try {
            for (int i = 0; i < specs.size(); i++) {
                ctx.getCatalogManager()
                        .getCatalogOrThrowException(getTableIdentifier().getCatalogName())
                        .createPartition(
                                tablePath,
                                specs.get(i),
                                partitions.get(i),
                                ignoreIfPartitionExists());
            }
            return TableResultImpl.TABLE_RESULT_OK;
        } catch (TableNotExistException e) {
            throw new ValidationException(
                    String.format("Could not execute %s", asSummaryString()), e);
        } catch (Exception e) {
            throw new TableException(String.format("Could not execute %s", asSummaryString()), e);
        }
    }
}
