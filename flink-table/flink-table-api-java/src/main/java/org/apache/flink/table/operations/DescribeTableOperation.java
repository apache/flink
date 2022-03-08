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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Operation to describe a DESCRIBE [EXTENDED] [[catalogName.] dataBasesName].sqlIdentifier
 * [PARTITION partitionSpec] statement.
 */
public class DescribeTableOperation implements Operation {

    private final ObjectIdentifier sqlIdentifier;
    private final boolean isExtended;
    private final CatalogPartitionSpec partitionSpec;

    public DescribeTableOperation(ObjectIdentifier sqlIdentifier, boolean isExtended) {
        this(sqlIdentifier, isExtended, null);
    }

    public DescribeTableOperation(
            ObjectIdentifier sqlIdentifier,
            boolean isExtended,
            CatalogPartitionSpec partitionSpec) {
        this.sqlIdentifier = sqlIdentifier;
        this.isExtended = isExtended;
        this.partitionSpec = partitionSpec;
    }

    public ObjectIdentifier getSqlIdentifier() {
        return sqlIdentifier;
    }

    public boolean isExtended() {
        return isExtended;
    }

    public Map<String, String> getPartitionSpec() {
        return partitionSpec == null
                ? Collections.emptyMap()
                : new LinkedHashMap<>(partitionSpec.getPartitionSpec());
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("identifier", sqlIdentifier);
        params.put("isExtended", isExtended);
        if (partitionSpec != null) {
            params.put("partition", OperationUtils.formatPartitionSpec(partitionSpec));
        }
        return OperationUtils.formatWithChildren(
                "DESCRIBE", params, Collections.emptyList(), Operation::asSummaryString);
    }
}
