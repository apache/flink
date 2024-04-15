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

import org.apache.flink.api.common.ExecutionConfig;
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
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformationWrapper;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ParallelismProvider;
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
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.state.KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

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
        final Transformation<RowData> sourceTransform;
        final StreamExecutionEnvironment env = planner.getExecEnv();
        final TransformationMetadata meta = createTransformationMeta(SOURCE_TRANSFORMATION, config);
        final InternalTypeInfo<RowData> outputTypeInfo =
                InternalTypeInfo.of((RowType) getOutputType());
        final ScanTableSource tableSource =
                tableSourceSpec.getScanTableSource(
                        planner.getFlinkContext(), ShortcutUtils.unwrapTypeFactory(planner));
        ScanTableSource.ScanRuntimeProvider provider =
                tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        final int sourceParallelism = deriveSourceParallelism(provider);
        final boolean sourceParallelismConfigured = isParallelismConfigured(provider);
        if (provider instanceof SourceFunctionProvider) {
            final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
            final SourceFunction<RowData> function = sourceFunctionProvider.createSourceFunction();
            sourceTransform =
                    createSourceFunctionTransformation(
                            env,
                            function,
                            sourceFunctionProvider.isBounded(),
                            meta.getName(),
                            outputTypeInfo,
                            sourceParallelism,
                            sourceParallelismConfigured);
            if (function instanceof ParallelSourceFunction && sourceParallelismConfigured) {
                meta.fill(sourceTransform);
                return new SourceTransformationWrapper<>(sourceTransform);
            } else {
                return meta.fill(sourceTransform);
            }
        } else if (provider instanceof InputFormatProvider) {
            final InputFormat<RowData, ?> inputFormat =
                    ((InputFormatProvider) provider).createInputFormat();
            sourceTransform =
                    createInputFormatTransformation(
                            env, inputFormat, outputTypeInfo, meta.getName());
            meta.fill(sourceTransform);
        } else if (provider instanceof SourceProvider) {
            final Source<RowData, ?, ?> source = ((SourceProvider) provider).createSource();
            // TODO: Push down watermark strategy to source scan
            sourceTransform =
                    env.fromSource(
                                    source,
                                    WatermarkStrategy.noWatermarks(),
                                    meta.getName(),
                                    outputTypeInfo)
                            .getTransformation();
            meta.fill(sourceTransform);
        } else if (provider instanceof DataStreamScanProvider) {
            sourceTransform =
                    ((DataStreamScanProvider) provider)
                            .produceDataStream(createProviderContext(config), env)
                            .getTransformation();
            meta.fill(sourceTransform);
            sourceTransform.setOutputType(outputTypeInfo);
        } else if (provider instanceof TransformationScanProvider) {
            sourceTransform =
                    ((TransformationScanProvider) provider)
                            .createTransformation(createProviderContext(config));
            meta.fill(sourceTransform);
            sourceTransform.setOutputType(outputTypeInfo);
        } else {
            throw new UnsupportedOperationException(
                    provider.getClass().getSimpleName() + " is unsupported now.");
        }

        if (sourceParallelismConfigured) {
            return applySourceTransformationWrapper(
                    sourceTransform,
                    planner.getFlinkContext().getClassLoader(),
                    outputTypeInfo,
                    config,
                    tableSource.getChangelogMode(),
                    sourceParallelism);
        } else {
            return sourceTransform;
        }
    }

    private boolean isParallelismConfigured(ScanTableSource.ScanRuntimeProvider runtimeProvider) {
        return runtimeProvider instanceof ParallelismProvider
                && ((ParallelismProvider) runtimeProvider).getParallelism().isPresent();
    }

    private int deriveSourceParallelism(ScanTableSource.ScanRuntimeProvider runtimeProvider) {
        if (isParallelismConfigured(runtimeProvider)) {
            int sourceParallelism = ((ParallelismProvider) runtimeProvider).getParallelism().get();
            if (sourceParallelism <= 0) {
                throw new TableException(
                        String.format(
                                "Invalid configured parallelism %s for table '%s'.",
                                sourceParallelism,
                                tableSourceSpec
                                        .getContextResolvedTable()
                                        .getIdentifier()
                                        .asSummaryString()));
            }
            return sourceParallelism;
        } else {
            return ExecutionConfig.PARALLELISM_DEFAULT;
        }
    }

    protected RowType getPhysicalRowType(ResolvedSchema schema) {
        return (RowType) schema.toPhysicalRowDataType().getLogicalType();
    }

    protected int[] getPrimaryKeyIndices(RowType sourceRowType, ResolvedSchema schema) {
        return schema.getPrimaryKey()
                .map(k -> k.getColumns().stream().mapToInt(sourceRowType::getFieldIndex).toArray())
                .orElse(new int[0]);
    }

    private Transformation<RowData> applySourceTransformationWrapper(
            Transformation<RowData> sourceTransform,
            ClassLoader classLoader,
            InternalTypeInfo<RowData> outputTypeInfo,
            ExecNodeConfig config,
            ChangelogMode changelogMode,
            int sourceParallelism) {
        sourceTransform.setParallelism(sourceParallelism, true);
        Transformation<RowData> sourceTransformationWrapper =
                new SourceTransformationWrapper<>(sourceTransform);

        if (!changelogMode.containsOnly(RowKind.INSERT)) {
            final ResolvedSchema schema =
                    tableSourceSpec.getContextResolvedTable().getResolvedSchema();
            final RowType physicalRowType = getPhysicalRowType(schema);
            final int[] primaryKeys = getPrimaryKeyIndices(physicalRowType, schema);
            final boolean hasPk = primaryKeys.length > 0;
            if (!hasPk) {
                throw new TableException(
                        String.format(
                                "Configured parallelism %s for upsert table '%s' while can not find primary key field. "
                                        + "This is a bug, please file an issue.",
                                sourceParallelism,
                                tableSourceSpec
                                        .getContextResolvedTable()
                                        .getIdentifier()
                                        .asSummaryString()));
            }
            final RowDataKeySelector selector =
                    KeySelectorUtil.getRowDataSelector(classLoader, primaryKeys, outputTypeInfo);
            final KeyGroupStreamPartitioner<RowData, RowData> partitioner =
                    new KeyGroupStreamPartitioner<>(selector, DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
            Transformation<RowData> partitionedTransform =
                    new PartitionTransformation<>(sourceTransformationWrapper, partitioner);
            createTransformationMeta("partitioner", "Partitioner", "Partitioner", config)
                    .fill(partitionedTransform);
            return partitionedTransform;
        } else {
            return sourceTransformationWrapper;
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
            TypeInformation<RowData> outputTypeInfo,
            int sourceParallelism,
            boolean sourceParallelismConfigured) {

        env.clean(function);

        final int parallelism;
        if (function instanceof ParallelSourceFunction) {
            if (sourceParallelismConfigured) {
                parallelism = sourceParallelism;
            } else {
                parallelism = env.getParallelism();
            }
        } else {
            parallelism = 1;
            sourceParallelismConfigured = true;
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
                sourceParallelismConfigured);
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
