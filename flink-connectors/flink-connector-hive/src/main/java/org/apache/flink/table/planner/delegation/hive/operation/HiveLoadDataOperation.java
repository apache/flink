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

package org.apache.flink.table.planner.delegation.hive.operation;

import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.operations.Operation;

import org.apache.hadoop.fs.Path;

import java.util.Map;

/**
 * An operation that loads data into a Hive table.
 *
 * <pre>The syntax is: {@code
 * LOAD DATA [LOCAL] INPATH 'filepath' [OVERWRITE] INTO TABLE tablename
 * [PARTITION (partcol1=val1, partcol2=val2 ...)]
 * }
 * </pre>
 */
public class HiveLoadDataOperation implements Operation {
    private final Path path;
    private final ObjectPath tablePath;
    private final boolean isOverwrite;
    private final boolean isSrcLocal;
    private final Map<String, String> partitionSpec;

    public HiveLoadDataOperation(
            Path path,
            ObjectPath tablePath,
            boolean isOverwrite,
            boolean isSrcLocal,
            Map<String, String> partitionSpec) {
        this.path = path;
        this.tablePath = tablePath;
        this.isOverwrite = isOverwrite;
        this.isSrcLocal = isSrcLocal;
        this.partitionSpec = partitionSpec;
    }

    public Path getPath() {
        return path;
    }

    public ObjectPath getTablePath() {
        return tablePath;
    }

    public boolean isOverwrite() {
        return isOverwrite;
    }

    public boolean isSrcLocal() {
        return isSrcLocal;
    }

    public Map<String, String> getPartitionSpec() {
        return partitionSpec;
    }

    @Override
    public String asSummaryString() {
        StringBuilder stringBuilder = new StringBuilder("LOAD DATA");
        if (isSrcLocal) {
            stringBuilder.append(" LOCAL");
        }
        stringBuilder
                .append(" INPATH")
                .append(String.format(" '%s'", path))
                .append(isOverwrite ? " OVERWRITE" : "")
                .append(" INTO TABLE ")
                .append(tablePath.getFullName());
        if (partitionSpec.size() > 0) {
            String[] pv = new String[partitionSpec.size()];
            int i = 0;
            for (Map.Entry<String, String> partition : partitionSpec.entrySet()) {
                pv[i++] = String.format("%s=%s", partition.getKey(), partition.getValue());
            }
            stringBuilder.append(" PARTITION (").append(String.join(", ", pv)).append(")");
        }
        return stringBuilder.toString();
    }
}
