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

package org.apache.flink.table.planner.connectors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsReadingMetadata;
import org.apache.flink.table.connector.source.abilities.SupportsSourceWatermark;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.source.InputConversionOperator;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.RowKind;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Table source for connecting to the external {@link DataStream} API. */
@Internal
final class ExternalDynamicSource<E>
        implements ScanTableSource, SupportsReadingMetadata, SupportsSourceWatermark {

    private static final String ROWTIME_METADATA_KEY = "rowtime";

    private static final DataType ROWTIME_METADATA_DATA_TYPE = DataTypes.TIMESTAMP_LTZ(3).notNull();

    private final ObjectIdentifier identifier;

    private final DataStream<E> dataStream;

    private final DataType physicalDataType;

    private final boolean isTopLevelRecord;

    private final ChangelogMode changelogMode;

    // mutable attributes

    private boolean produceRowtimeMetadata;

    private boolean propagateWatermark;

    ExternalDynamicSource(
            ObjectIdentifier identifier,
            DataStream<E> dataStream,
            DataType physicalDataType,
            boolean isTopLevelRecord,
            ChangelogMode changelogMode) {
        this.identifier = identifier;
        this.dataStream = dataStream;
        this.physicalDataType = physicalDataType;
        this.isTopLevelRecord = isTopLevelRecord;
        this.changelogMode = changelogMode;
    }

    @Override
    public DynamicTableSource copy() {
        final ExternalDynamicSource<E> copy =
                new ExternalDynamicSource<>(
                        identifier, dataStream, physicalDataType, isTopLevelRecord, changelogMode);
        copy.produceRowtimeMetadata = produceRowtimeMetadata;
        copy.propagateWatermark = propagateWatermark;
        return copy;
    }

    @Override
    public String asSummaryString() {
        return generateOperatorName();
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return changelogMode;
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        final DataStructureConverter physicalConverter =
                runtimeProviderContext.createDataStructureConverter(physicalDataType);

        final Transformation<E> externalTransformation = dataStream.getTransformation();

        final Transformation<RowData> conversionTransformation =
                new OneInputTransformation<>(
                        externalTransformation,
                        generateOperatorName(),
                        new InputConversionOperator<>(
                                physicalConverter,
                                !isTopLevelRecord,
                                produceRowtimeMetadata,
                                propagateWatermark,
                                changelogMode.containsOnly(RowKind.INSERT)),
                        null, // will be filled by the framework
                        externalTransformation.getParallelism());

        return TransformationScanProvider.of(conversionTransformation);
    }

    private String generateOperatorName() {
        return String.format(
                "DataSteamToTable(stream=%s, type=%s, rowtime=%s, watermark=%s)",
                identifier.asSummaryString(),
                physicalDataType.toString(),
                produceRowtimeMetadata,
                propagateWatermark);
    }

    @Override
    public Map<String, DataType> listReadableMetadata() {
        return Collections.singletonMap(ROWTIME_METADATA_KEY, ROWTIME_METADATA_DATA_TYPE);
    }

    @Override
    public void applyReadableMetadata(List<String> metadataKeys, DataType producedDataType) {
        produceRowtimeMetadata = metadataKeys.contains(ROWTIME_METADATA_KEY);
    }

    @Override
    public void applySourceWatermark() {
        propagateWatermark = true;
    }
}
