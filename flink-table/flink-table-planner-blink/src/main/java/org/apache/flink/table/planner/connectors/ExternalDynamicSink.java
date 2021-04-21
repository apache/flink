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
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.sink.OutputConversionOperator;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

/** Table sink for connecting to the external {@link DataStream} API. */
@Internal
final class ExternalDynamicSink implements DynamicTableSink {

    private final ChangelogMode changelogMode;

    private final DataType physicalDataType;

    ExternalDynamicSink(ChangelogMode changelogMode, DataType physicalDataType) {
        this.changelogMode = changelogMode;
        this.physicalDataType = physicalDataType;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
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

                    return new OneInputTransformation<>(
                            input,
                            generateOperatorName(),
                            new OutputConversionOperator(
                                    atomicFieldGetter,
                                    physicalConverter,
                                    transformationContext.getRowtimeIndex()),
                            ExternalTypeInfo.of(physicalDataType),
                            input.getParallelism());
                };
    }

    private String generateOperatorName() {
        return String.format("TableToDataSteam(type=%s)", physicalDataType.toString());
    }

    @Override
    public DynamicTableSink copy() {
        return new ExternalDynamicSink(changelogMode, physicalDataType);
    }

    @Override
    public String asSummaryString() {
        return generateOperatorName();
    }
}
