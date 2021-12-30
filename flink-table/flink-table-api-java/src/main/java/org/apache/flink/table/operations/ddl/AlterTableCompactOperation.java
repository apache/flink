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

import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.OperationUtils;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Operation to describe "ALTER TABLE [PARTITION partition_spec] COMPACT" statement. */
public class AlterTableCompactOperation extends AlterTableOperation {

    private final CatalogPartitionSpec partitionSpec;

    public AlterTableCompactOperation(
            ObjectIdentifier tableIdentifier, @Nullable CatalogPartitionSpec partitionSpec) {
        super(tableIdentifier);
        this.partitionSpec = partitionSpec;
    }

    public Map<String, String> getPartitionSpec() {
        return partitionSpec == null
                ? Collections.emptyMap()
                : new LinkedHashMap<>(partitionSpec.getPartitionSpec());
    }

    @Override
    public String asSummaryString() {
        String spec =
                partitionSpec == null
                        ? ""
                        : String.format(
                                "PARTITION (%s) ",
                                OperationUtils.formatPartitionSpec(partitionSpec));
        return String.format("ALTER TABLE %s %sCOMPACT", tableIdentifier.asSummaryString(), spec);
    }
}
