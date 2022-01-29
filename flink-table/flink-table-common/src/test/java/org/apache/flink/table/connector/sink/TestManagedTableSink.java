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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.abilities.SupportsOverwrite;
import org.apache.flink.table.connector.sink.abilities.SupportsPartitioning;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Managed {@link DynamicTableSink} for testing. */
public class TestManagedTableSink
        implements DynamicTableSink, SupportsOverwrite, SupportsPartitioning {

    private final DynamicTableFactory.Context context;
    private final Path basePath;

    private LinkedHashMap<String, String> staticPartitionSpecs = new LinkedHashMap<>();
    private boolean overwrite = false;

    public TestManagedTableSink(DynamicTableFactory.Context context, Path basePath) {
        this.context = context;
        this.basePath = basePath;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.insertOnly();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkProvider.of(new TestManagedSink(this.context.getObjectIdentifier(), basePath));
    }

    @Override
    public DynamicTableSink copy() {
        TestManagedTableSink copied = new TestManagedTableSink(context, basePath);
        copied.overwrite = this.overwrite;
        copied.staticPartitionSpecs = this.staticPartitionSpecs;
        return copied;
    }

    @Override
    public String asSummaryString() {
        return "TestManagedTableSink";
    }

    @Override
    public void applyOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    @Override
    public void applyStaticPartition(Map<String, String> partition) {
        List<String> partitionKeys = context.getCatalogTable().getPartitionKeys();
        for (String partitionKey : partitionKeys) {
            if (partition.containsKey(partitionKey)) {
                staticPartitionSpecs.put(partitionKey, partition.get(partitionKey));
            }
        }
    }
}
