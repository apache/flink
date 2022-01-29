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

package org.apache.flink.table.connector.sink;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.data.RowData;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Committable which contains the generated compact files to be created and the old files to be
 * deleted.
 */
public class TestManagedCommittable {

    private final Map<CatalogPartitionSpec, List<RowData>> toAdd;
    private final Map<CatalogPartitionSpec, Set<Path>> toDelete;

    public TestManagedCommittable(
            Map<CatalogPartitionSpec, List<RowData>> toAdd,
            Map<CatalogPartitionSpec, Set<Path>> toDelete) {
        this.toAdd = toAdd;
        this.toDelete = toDelete;
    }

    public Map<CatalogPartitionSpec, List<RowData>> getToAdd() {
        return toAdd;
    }

    public Map<CatalogPartitionSpec, Set<Path>> getToDelete() {
        return toDelete;
    }

    public static TestManagedCommittable combine(List<TestManagedCommittable> committables) {
        Map<CatalogPartitionSpec, List<RowData>> toAdd = new HashMap<>();
        Map<CatalogPartitionSpec, Set<Path>> toDelete = new HashMap<>();
        for (TestManagedCommittable committable : committables) {
            Map<CatalogPartitionSpec, List<RowData>> partialAdd = committable.toAdd;
            Map<CatalogPartitionSpec, Set<Path>> partialDelete = committable.toDelete;

            for (Map.Entry<CatalogPartitionSpec, List<RowData>> entry : partialAdd.entrySet()) {
                CatalogPartitionSpec partitionSpec = entry.getKey();
                List<RowData> elements = toAdd.getOrDefault(partitionSpec, new ArrayList<>());
                elements.addAll(entry.getValue());
                toAdd.put(partitionSpec, elements);
            }

            for (Map.Entry<CatalogPartitionSpec, Set<Path>> entry : partialDelete.entrySet()) {
                CatalogPartitionSpec partitionSpec = entry.getKey();
                Set<Path> paths = toDelete.getOrDefault(partitionSpec, new HashSet<>());
                paths.addAll(entry.getValue());
                toDelete.put(partitionSpec, paths);
            }
        }
        return new TestManagedCommittable(toAdd, toDelete);
    }
}
