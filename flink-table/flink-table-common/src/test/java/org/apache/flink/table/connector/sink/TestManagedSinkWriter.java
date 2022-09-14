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

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.utils.PartitionPathUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Managed {@link SinkWriter} for testing compaction. */
public class TestManagedSinkWriter implements SinkWriter<RowData, TestManagedCommittable, Void> {

    private final Map<String, CatalogPartitionSpec> processedPartitions = new HashMap<>();
    private final Map<CatalogPartitionSpec, List<RowData>> stagingElements = new HashMap<>();
    private final Map<CatalogPartitionSpec, Set<Path>> toDelete = new HashMap<>();

    @Override
    public void write(RowData element, Context context) throws IOException, InterruptedException {
        assertThat(element.getArity()).isEqualTo(3);
        String partition = element.getString(0).toString();
        Path filePath = new Path(element.getString(1).toString());
        RowData rowData = GenericRowData.of(element.getString(2));
        CatalogPartitionSpec currentPartitionSpec =
                processedPartitions.getOrDefault(
                        partition,
                        new CatalogPartitionSpec(
                                PartitionPathUtils.extractPartitionSpecFromPath(filePath)));
        processedPartitions.put(partition, currentPartitionSpec);
        List<RowData> elements =
                stagingElements.getOrDefault(currentPartitionSpec, new ArrayList<>());
        elements.add(rowData);
        stagingElements.put(currentPartitionSpec, elements);
        Set<Path> old = toDelete.getOrDefault(currentPartitionSpec, new HashSet<>());
        old.add(filePath);
        toDelete.put(currentPartitionSpec, old);
    }

    @Override
    public List<TestManagedCommittable> prepareCommit(boolean flush)
            throws IOException, InterruptedException {
        return Collections.singletonList(new TestManagedCommittable(stagingElements, toDelete));
    }

    @Override
    public void close() throws Exception {}
}
