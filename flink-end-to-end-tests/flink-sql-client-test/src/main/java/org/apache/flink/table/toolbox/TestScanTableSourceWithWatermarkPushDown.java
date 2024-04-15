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

package org.apache.flink.table.toolbox;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;

/**
 * A source used to test {@link SupportsWatermarkPushDown}.
 *
 * <p>For simplicity, the deprecated source function method is used to create the source.
 */
public class TestScanTableSourceWithWatermarkPushDown
        implements ScanTableSource, SupportsWatermarkPushDown {

    private WatermarkStrategy<RowData> watermarkStrategy;

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public DynamicTableSource copy() {
        final TestScanTableSourceWithWatermarkPushDown newSource =
                new TestScanTableSourceWithWatermarkPushDown();
        newSource.watermarkStrategy = this.watermarkStrategy;
        return newSource;
    }

    @Override
    public String asSummaryString() {
        return "TestScanTableSourceWithWatermarkPushDown";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        return SourceFunctionProvider.of(new TestSourceFunction(watermarkStrategy), false);
    }
}
