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

package org.apache.flink.table.planner.factories.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;

/** Values {@link ScanTableSource} which collects the registered {@link RowData} directly. */
public class TestValuesScanTableSourceWithInternalData implements ScanTableSource {
    private final String dataId;
    private final boolean bounded;

    public TestValuesScanTableSourceWithInternalData(String dataId, boolean bounded) {
        this.dataId = dataId;
        this.bounded = bounded;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final SourceFunction<RowData> sourceFunction = new FromRowDataSourceFunction(dataId);
        return SourceFunctionProvider.of(sourceFunction, bounded);
    }

    @Override
    public DynamicTableSource copy() {
        return new TestValuesScanTableSourceWithInternalData(dataId, bounded);
    }

    @Override
    public String asSummaryString() {
        return "TestValuesWithInternalData";
    }
}
