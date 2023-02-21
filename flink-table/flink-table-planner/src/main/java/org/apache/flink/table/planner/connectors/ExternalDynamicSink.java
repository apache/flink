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
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.abilities.SupportsWritingMetadata;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.runtime.operators.sink.OutputConversionOperator;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/** Table sink for connecting to the external {@link DataStream} API. */
@Internal
final class ExternalDynamicSink implements DynamicTableSink, SupportsWritingMetadata {

    private static final String EXTERNAL_DATASTREAM_TRANSFORMATION = "external-datastream";

    private static final String ROWTIME_METADATA_KEY = "rowtime";

    private static final DataType ROWTIME_METADATA_DATA_TYPE = DataTypes.TIMESTAMP_LTZ(3).notNull();

    private final @Nullable ChangelogMode changelogMode;

    private final DataType physicalDataType;

    // mutable attributes

    private boolean consumeRowtimeMetadata;

    ExternalDynamicSink(@Nullable ChangelogMode changelogMode, DataType physicalDataType) {
        this.changelogMode = changelogMode;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        if (changelogMode == null) {
            return requestedMode;
        }
        return changelogMode;
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        final DynamicTableSink.DataStructureConverter physicalConverter =
                context.createDataStructureConverter(physicalDataType);
        return (TransformationSinkProvider)
                transformationContext -> {
                    final Transformation<RowData> input =
                            transformationContext.getInputTransformation();

                    final LogicalType physicalType = physicalDataType.getLogicalType();

                    final RowData.FieldGetter atomicFieldGetter;
                    if (LogicalTypeChecks.isCompositeType(physicalType)) {
                        atomicFieldGetter = null;
                    } else {
                        atomicFieldGetter = RowData.createFieldGetter(physicalType, 0);
                    }

                    TransformationMetadata transformationMeta =
                            transformationContext
                                    .generateUid(EXTERNAL_DATASTREAM_TRANSFORMATION)
                                    .map(
                                            uid ->
                                                    new TransformationMetadata(
                                                            uid,
                                                            generateOperatorName(),
                                                            generateOperatorDesc()))
                                    .orElseGet(
                                            () ->
                                                    new TransformationMetadata(
                                                            generateOperatorName(),
                                                            generateOperatorDesc()));

                    return ExecNodeUtil.createOneInputTransformation(
                            input,
                            transformationMeta,
                            new OutputConversionOperator(
                                    atomicFieldGetter,
                                    physicalConverter,
                                    transformationContext.getRowtimeIndex(),
                                    consumeRowtimeMetadata),
                            ExternalTypeInfo.of(physicalDataType),
                            input.getParallelism(),
                            false);
                };
    }

    private String generateOperatorName() {
        return "TableToDataSteam";
    }

    private String generateOperatorDesc() {
        return String.format(
                "TableToDataSteam(type=%s, rowtime=%s)",
                physicalDataType.toString(), consumeRowtimeMetadata);
    }

    @Override
    public DynamicTableSink copy() {
        return new ExternalDynamicSink(changelogMode, physicalDataType);
    }

    @Override
    public String asSummaryString() {
        return generateOperatorName();
    }

    @Override
    public Map<String, DataType> listWritableMetadata() {
        return Collections.singletonMap(ROWTIME_METADATA_KEY, ROWTIME_METADATA_DATA_TYPE);
    }

    @Override
    public void applyWritableMetadata(List<String> metadataKeys, DataType consumedDataType) {
        consumeRowtimeMetadata = metadataKeys.contains(ROWTIME_METADATA_KEY);
    }
}
