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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.RuntimeConverter;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.connector.source.abilities.SupportsWatermarkPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Values {@link ScanTableSource} for testing that supports watermark push down. */
public class TestValuesScanTableSourceWithWatermarkPushDown extends TestValuesScanTableSource
        implements SupportsWatermarkPushDown, SupportsSourceWatermark {
    private final String tableName;

    private WatermarkStrategy<RowData> watermarkStrategy;

    public TestValuesScanTableSourceWithWatermarkPushDown(
            DataType producedDataType,
            ChangelogMode changelogMode,
            String runtimeSource,
            boolean failingSource,
            Map<Map<String, String>, Collection<Row>> data,
            String tableName,
            boolean nestedProjectionSupported,
            @Nullable int[][] projectedPhysicalFields,
            List<ResolvedExpression> filterPredicates,
            Set<String> filterableFields,
            int numElementToSkip,
            long limit,
            List<Map<String, String>> allPartitions,
            Map<String, DataType> readableMetadata,
            @Nullable int[] projectedMetadataFields) {
        super(
                producedDataType,
                changelogMode,
                false,
                runtimeSource,
                failingSource,
                data,
                nestedProjectionSupported,
                projectedPhysicalFields,
                filterPredicates,
                filterableFields,
                numElementToSkip,
                limit,
                allPartitions,
                readableMetadata,
                projectedMetadataFields);
        this.tableName = tableName;
    }

    @Override
    public void applyWatermark(WatermarkStrategy<RowData> watermarkStrategy) {
        this.watermarkStrategy = watermarkStrategy;
    }

    @Override
    public void applySourceWatermark() {
        this.watermarkStrategy = WatermarkStrategy.noWatermarks();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        TypeInformation<RowData> type =
                runtimeProviderContext.createTypeInformation(producedDataType);
        TypeSerializer<RowData> serializer = type.createSerializer(new ExecutionConfig());
        DataStructureConverter converter =
                runtimeProviderContext.createDataStructureConverter(producedDataType);
        converter.open(
                RuntimeConverter.Context.create(TestValuesTableFactory.class.getClassLoader()));
        Collection<RowData> values = convertToRowData(converter, true);
        try {
            return SourceFunctionProvider.of(
                    new FromElementSourceFunctionWithWatermark(
                            tableName, serializer, values, watermarkStrategy),
                    false);
        } catch (IOException e) {
            throw new TableException("Fail to init source function", e);
        }
    }

    @Override
    public DynamicTableSource copy() {
        final TestValuesScanTableSourceWithWatermarkPushDown newSource =
                new TestValuesScanTableSourceWithWatermarkPushDown(
                        producedDataType,
                        changelogMode,
                        runtimeSource,
                        failingSource,
                        data,
                        tableName,
                        nestedProjectionSupported,
                        projectedPhysicalFields,
                        filterPredicates,
                        filterableFields,
                        numElementToSkip,
                        limit,
                        allPartitions,
                        readableMetadata,
                        projectedMetadataFields);
        newSource.watermarkStrategy = watermarkStrategy;
        return newSource;
    }
}
