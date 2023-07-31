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
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.source.DataStreamScanProvider;
import org.apache.flink.table.connector.source.InputFormatProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.connectors.TransformationScanProvider;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

/**
 * Base {@link ExecNode} to read data from an external source defined by a {@link ScanTableSource}.
 */
public abstract class CommonExecTableSourceScan extends ExecNodeBase<RowData>
        implements MultipleTransformationTranslator<RowData> {

    public static final String SOURCE_TRANSFORMATION = "source";

    public static final String FIELD_NAME_SCAN_TABLE_SOURCE = "scanTableSource";

    @JsonProperty(FIELD_NAME_SCAN_TABLE_SOURCE)
    private final DynamicTableSourceSpec tableSourceSpec;

    protected CommonExecTableSourceScan(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            DynamicTableSourceSpec tableSourceSpec,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.tableSourceSpec = tableSourceSpec;
    }

    @Override
    public String getSimplifiedName() {
        return tableSourceSpec.getContextResolvedTable().getIdentifier().getObjectName();
    }

    public DynamicTableSourceSpec getTableSourceSpec() {
        return tableSourceSpec;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final StreamExecutionEnvironment env = planner.getExecEnv();
        final TransformationMetadata meta = createTransformationMeta(SOURCE_TRANSFORMATION, config);
        final InternalTypeInfo<RowData> outputTypeInfo =
                InternalTypeInfo.of((RowType) getOutputType());
        final ScanTableSource tableSource =
                tableSourceSpec.getScanTableSource(
                        planner.getFlinkContext(), ShortcutUtils.unwrapTypeFactory(planner));
        ScanTableSource.ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        if (provider instanceof SourceFunctionProvider) {
            final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
            final SourceFunction<RowData> function = sourceFunctionProvider.createSourceFunction();
            final Transformation<RowData> transformation =
                    createSourceFunctionTransformation(
                            env,
                            function,
                            sourceFunctionProvider.isBounded(),
                            meta.getName(),
                            outputTypeInfo);
            return meta.fill(transformation);
        } else if (provider instanceof InputFormatProvider) {
            final InputFormat<RowData, ?> inputFormat =
                    ((InputFormatProvider) provider).createInputFormat();
            final Transformation<RowData> transformation =
                    createInputFormatTransformation(
                            env, inputFormat, outputTypeInfo, meta.getName());
            return meta.fill(transformation);
        } else if (provider instanceof SourceProvider) {
            final Source<RowData, ?, ?> source = ((SourceProvider) provider).createSource();
            // TODO: Push down watermark strategy to source scan
            final Transformation<RowData> transformation =
                    env.fromSource(
                                    source,
                                    WatermarkStrategy.noWatermarks(),
                                    meta.getName(),
                                    outputTypeInfo)
                            .getTransformation();
            return meta.fill(transformation);
        } else if (provider instanceof DataStreamScanProvider) {
            Transformation<RowData> transformation =
                    ((DataStreamScanProvider) provider)
                            .produceDataStream(createProviderContext(config), env)
                            .getTransformation();
            meta.fill(transformation);
            transformation.setOutputType(outputTypeInfo);
            return transformation;
        } else if (provider instanceof TransformationScanProvider) {
            final Transformation<RowData> transformation =
                    ((TransformationScanProvider) provider)
                            .createTransformation(createProviderContext(config));
            meta.fill(transformation);
            transformation.setOutputType(outputTypeInfo);
            return transformation;
        } else {
            throw new UnsupportedOperationException(
                    provider.getClass().getSimpleName() + " is unsupported now.");
        }
    }

    private ProviderContext createProviderContext(ExecNodeConfig config) {
        return name -> {
            if (this instanceof StreamExecNode && config.shouldSetUid()) {
                return Optional.of(createTransformationUid(name, config));
            }
            return Optional.empty();
        };
    }

    /**
     * Adopted from {@link StreamExecutionEnvironment#addSource(SourceFunction, String,
     * TypeInformation)} but with custom {@link Boundedness}.
     *
     * @deprecated This method relies on the {@link
     *     org.apache.flink.streaming.api.functions.source.SourceFunction} API, which is due to be
     *     removed.
     */
    @Deprecated
    protected Transformation<RowData> createSourceFunctionTransformation(
            StreamExecutionEnvironment env,
            SourceFunction<RowData> function,
            boolean isBounded,
            String operatorName,
            TypeInformation<RowData> outputTypeInfo) {

        env.clean(function);

        final int parallelism;
        boolean parallelismConfigured = false;
        if (function instanceof ParallelSourceFunction) {
            parallelism = env.getParallelism();
        } else {
            parallelism = 1;
            parallelismConfigured = true;
        }

        final Boundedness boundedness;
        if (isBounded) {
            boundedness = Boundedness.BOUNDED;
        } else {
            boundedness = Boundedness.CONTINUOUS_UNBOUNDED;
        }

        final StreamSource<RowData, ?> sourceOperator = new StreamSource<>(function, !isBounded);
        return new LegacySourceTransformation<>(
                operatorName,
                sourceOperator,
                outputTypeInfo,
                parallelism,
                boundedness,
                parallelismConfigured);
    }

    /**
     * Creates a {@link Transformation} based on the given {@link InputFormat}. The implementation
     * is different for streaming mode and batch mode.
     */
    protected abstract Transformation<RowData> createInputFormatTransformation(
            StreamExecutionEnvironment env,
            InputFormat<RowData, ?> inputFormat,
            InternalTypeInfo<RowData> outputTypeInfo,
            String operatorName);
}
