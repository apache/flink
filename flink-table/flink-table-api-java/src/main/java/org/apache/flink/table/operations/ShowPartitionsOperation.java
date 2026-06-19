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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.internal.TableResultUtils.buildStringArrayResult;

/** Operation to describe a SHOW PARTITIONS statement. */
@Internal
public class ShowPartitionsOperation implements ShowOperation {

    protected final ObjectIdentifier tableIdentifier;
    @Nullable private final CatalogPartitionSpec partitionSpec;
    // the name for the default partition, which usually means the partition's value is null or
    // empty string
    @Nullable private final String defaultPartitionName;

    public ShowPartitionsOperation(
            ObjectIdentifier tableIdentifier, @Nullable CatalogPartitionSpec partitionSpec) {
        this(tableIdentifier, partitionSpec, null);
    }

    public ShowPartitionsOperation(
            ObjectIdentifier tableIdentifier,
            @Nullable CatalogPartitionSpec partitionSpec,
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

    @Override
    public TableResultInternal execute(Context ctx) {
        try {
            final ObjectPath tablePath = tableIdentifier.toObjectPath();
            Catalog catalog =
                    ctx.getCatalogManager()
                            .getCatalogOrThrowException(tableIdentifier.getCatalogName());
            List<CatalogPartitionSpec> partitionSpecs =
                    partitionSpec == null
                            ? catalog.listPartitions(tablePath)
                            : catalog.listPartitions(tablePath, partitionSpec);
            List<String> partitionNames = new ArrayList<>(partitionSpecs.size());
            for (CatalogPartitionSpec spec : partitionSpecs) {
                List<String> partitionKVs = new ArrayList<>(spec.getPartitionSpec().size());
                for (Map.Entry<String, String> partitionKV : spec.getPartitionSpec().entrySet()) {
                    String partitionValue =
                            partitionKV.getValue() == null
                                    ? defaultPartitionName
                                    : partitionKV.getValue();
                    partitionKVs.add(partitionKV.getKey() + "=" + partitionValue);
                }
                partitionNames.add(String.join("/", partitionKVs));
            }
            return buildStringArrayResult("partition name", partitionNames.toArray(new String[0]));
        } catch (TableNotExistException e) {
            throw new ValidationException(
                    String.format("Could not execute %s", asSummaryString()), e);
        } catch (Exception e) {
            throw new TableException(String.format("Could not execute %s", asSummaryString()), e);
        }
    }
}
