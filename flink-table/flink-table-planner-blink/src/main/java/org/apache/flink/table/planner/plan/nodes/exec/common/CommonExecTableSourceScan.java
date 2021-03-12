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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;

/**
 * Base {@link ExecNode} to read data from an external source defined by a {@link ScanTableSource}.
 */
public abstract class CommonExecTableSourceScan extends ExecNodeBase<RowData>
        implements MultipleTransformationTranslator<RowData> {
    public static final String FIELD_NAME_SCAN_TABLE_SOURCE = "scanTableSource";

    @JsonProperty(FIELD_NAME_SCAN_TABLE_SOURCE)
    private final DynamicTableSourceSpec tableSourceSpec;

    protected CommonExecTableSourceScan(
            DynamicTableSourceSpec tableSourceSpec,
            int id,
            LogicalType outputType,
            String description) {
        super(id, Collections.emptyList(), outputType, description);
        this.tableSourceSpec = tableSourceSpec;
    }

    public DynamicTableSourceSpec getTableSourceSpec() {
        return tableSourceSpec;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final StreamExecutionEnvironment env = planner.getExecEnv();
        final String operatorName = getDescription();
        final InternalTypeInfo<RowData> outputTypeInfo =
                InternalTypeInfo.of((RowType) getOutputType());
        final ScanTableSource tableSource = tableSourceSpec.getScanTableSource(planner);
        ScanTableSource.ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        if (provider instanceof SourceFunctionProvider) {
            SourceFunction<RowData> sourceFunction =
                    ((SourceFunctionProvider) provider).createSourceFunction();
            return env.addSource(sourceFunction, operatorName, outputTypeInfo).getTransformation();
        } else if (provider instanceof InputFormatProvider) {
            InputFormat<RowData, ?> inputFormat =
                    ((InputFormatProvider) provider).createInputFormat();
            return createInputFormatTransformation(env, inputFormat, outputTypeInfo, operatorName);
        } else if (provider instanceof SourceProvider) {
            Source<RowData, ?, ?> source = ((SourceProvider) provider).createSource();
            // TODO: Push down watermark strategy to source scan
            return env.fromSource(
                            source, WatermarkStrategy.noWatermarks(), operatorName, outputTypeInfo)
                    .getTransformation();
        } else if (provider instanceof DataStreamScanProvider) {
            Transformation<RowData> transformation =
                    ((DataStreamScanProvider) provider).produceDataStream(env).getTransformation();
            transformation.setOutputType(outputTypeInfo);
            return transformation;
        } else {
            throw new UnsupportedOperationException(
                    provider.getClass().getSimpleName() + " is unsupported now.");
        }
    }

    /**
     * Creates a {@link Transformation} based on the given {@link InputFormat}. The implementation
     * is different for streaming mode and batch mode.
     */
    protected abstract Transformation<RowData> createInputFormatTransformation(
            StreamExecutionEnvironment env,
            InputFormat<RowData, ?> inputFormat,
            InternalTypeInfo<RowData> outputTypeInfo,
            String name);
}
