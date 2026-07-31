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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.datagen.functions.FromElementsGeneratorFunction;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.DataType;

import java.util.Arrays;
import java.util.List;

/** A source used to test {@link SupportsWatermarkPushDown}. */
public class TestScanTableSourceWithWatermarkPushDown
        implements ScanTableSource, SupportsWatermarkPushDown {

    public static final List<RowData> DATA =
            Arrays.asList(
                    GenericRowData.of(
                            StringData.fromString("Bob"), 1L, TimestampData.fromEpochMillis(1)),
                    GenericRowData.of(
                            StringData.fromString("Alice"), 2L, TimestampData.fromEpochMillis(2)));

    private final DataType producedDataType;

    private WatermarkStrategy<RowData> watermarkStrategy;

    public TestScanTableSourceWithWatermarkPushDown(DataType producedDataType) {
        this.producedDataType = producedDataType;
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public DynamicTableSource copy() {
        final TestScanTableSourceWithWatermarkPushDown newSource =
                new TestScanTableSourceWithWatermarkPushDown(producedDataType);
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
        final TypeInformation<RowData> typeInfo =
                runtimeProviderContext.createTypeInformation(producedDataType);
        final DataGeneratorSource<RowData> source =
                new DataGeneratorSource<>(
                        new FromElementsGeneratorFunction<>(typeInfo, DATA.toArray(new RowData[0])),
                        DATA.size(),
                        typeInfo);
        // the pushed watermark strategy must be applied by the connector itself (like
        // KafkaDynamicSource); the planner does not apply it for SourceProvider sources
        return new DataStreamScanProvider() {
            @Override
            public DataStream<RowData> produceDataStream(
                    ProviderContext providerContext, StreamExecutionEnvironment execEnv) {
                final WatermarkStrategy<RowData> strategy =
                        watermarkStrategy != null
                                ? watermarkStrategy
                                : WatermarkStrategy.noWatermarks();
                return execEnv.fromSource(source, strategy, "watermark-push-down-source", typeInfo);
            }

            @Override
            public boolean isBounded() {
                return false;
            }
        };
    }
}
