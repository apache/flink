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

package org.apache.flink.table.connector.source;

import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.factories.DynamicTableFactory;

import java.util.Objects;

/** Managed {@link DynamicTableSource} for testing. */
public class TestManagedTableSource implements ScanTableSource {

    private final DynamicTableFactory.Context context;
    private final CompactPartitions partitions;
    private final ChangelogMode changelogMode;

    public TestManagedTableSource(
            DynamicTableFactory.Context context,
            CompactPartitions partitions,
            ChangelogMode changelogMode) {
        this.context = context;
        this.partitions = partitions;
        this.changelogMode = changelogMode;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return changelogMode;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return SourceProvider.of(new TestManagedSource(partitions));
    }

    @Override
    public DynamicTableSource copy() {
        return new TestManagedTableSource(context, partitions, changelogMode);
    }

    @Override
    public String asSummaryString() {
        return "TestManagedTableSource";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TestManagedTableSource)) {
            return false;
        }
        final TestManagedTableSource that = (TestManagedTableSource) o;
        return context.equals(that.context)
                && partitions.equals(that.partitions)
                && changelogMode.equals(that.changelogMode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(context, partitions, changelogMode);
    }
}
