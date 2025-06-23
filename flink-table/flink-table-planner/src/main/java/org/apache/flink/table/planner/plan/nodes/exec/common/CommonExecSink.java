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

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.lineage.LineageDataset;
import org.apache.flink.streaming.api.lineage.LineageVertex;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.TransformationWithLineage;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.ProviderContext;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.connector.sink.TransformationSinkProvider;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelDelete;
import org.apache.flink.table.connector.sink.abilities.SupportsRowLevelUpdate;
import org.apache.flink.table.connector.sink.legacy.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.lineage.TableLineageUtils;
import org.apache.flink.table.planner.lineage.TableSinkLineageVertex;
import org.apache.flink.table.planner.lineage.TableSinkLineageVertexImpl;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelDeleteSpec;
import org.apache.flink.table.planner.plan.abilities.sink.RowLevelUpdateSpec;
import org.apache.flink.table.planner.plan.abilities.sink.SinkAbilitySpec;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.sink.RowKindSetter;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter;
import org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcer;
import org.apache.flink.table.runtime.operators.sink.constraint.ConstraintEnforcerExecutor;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.TemporaryClassLoaderContext;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Base {@link ExecNode} to write data to an external sink defined by a {@link DynamicTableSink}.
 */
public abstract class CommonExecSink extends ExecNodeBase<Object>
        implements MultipleTransformationTranslator<Object> {

    public static final String CONSTRAINT_VALIDATOR_TRANSFORMATION = "constraint-validator";
    public static final String PARTITIONER_TRANSFORMATION = "partitioner";
    public static final String UPSERT_MATERIALIZE_TRANSFORMATION = "upsert-materialize";
    public static final String TIMESTAMP_INSERTER_TRANSFORMATION = "timestamp-inserter";
    public static final String ROW_KIND_SETTER = "row-kind-setter";
    public static final String SINK_TRANSFORMATION = "sink";

    public static final String FIELD_NAME_DYNAMIC_TABLE_SINK = "dynamicTableSink";

    @JsonProperty(FIELD_NAME_DYNAMIC_TABLE_SINK)
    protected final DynamicTableSinkSpec tableSinkSpec;

    private final ChangelogMode inputChangelogMode;
    private final boolean isBounded;
    protected boolean sinkParallelismConfigured;

    protected CommonExecSink(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode inputChangelogMode,
            boolean isBounded,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(id, context, persistedConfig, inputProperties, outputType, description);
        this.tableSinkSpec = tableSinkSpec;
        this.inputChangelogMode = inputChangelogMode;
        this.isBounded = isBounded;
    }

    @Override
    public String getSimplifiedName() {
        return tableSinkSpec.getContextResolvedTable().getIdentifier().getObjectName();
    }

    public DynamicTableSinkSpec getTableSinkSpec() {
        return tableSinkSpec;
    }

    @SuppressWarnings("unchecked")
    protected Transformation<Object> createSinkTransformation(
            StreamExecutionEnvironment streamExecEnv,
            ExecNodeConfig config,
            ClassLoader classLoader,
            Transformation<RowData> inputTransform,
            DynamicTableSink tableSink,
            int rowtimeFieldIndex,
            boolean upsertMaterialize,
            int[] inputUpsertKey) {
        final ResolvedSchema schema = tableSinkSpec.getContextResolvedTable().getResolvedSchema();
        final SinkRuntimeProvider runtimeProvider =
                tableSink.getSinkRuntimeProvider(
                        new SinkRuntimeProviderContext(
                                isBounded, tableSinkSpec.getTargetColumns()));
        final RowType physicalRowType = getPhysicalRowType(schema);
        final int[] primaryKeys = getPrimaryKeyIndices(physicalRowType, schema);
        final int sinkParallelism = deriveSinkParallelism(inputTransform, runtimeProvider);
        sinkParallelismConfigured = isParallelismConfigured(runtimeProvider);
        final int inputParallelism = inputTransform.getParallelism();
        final boolean inputInsertOnly = inputChangelogMode.containsOnly(RowKind.INSERT);
        final boolean hasPk = primaryKeys.length > 0;

        if (!inputInsertOnly && sinkParallelism != inputParallelism && !hasPk) {
            throw new TableException(
                    String.format(
                            "The sink for table '%s' has a configured parallelism of %s, while the input parallelism is %s. "
                                    + "Since the configured parallelism is different from the input's parallelism and "
                                    + "the changelog mode is not insert-only, a primary key is required but could not "
                                    + "be found.",
                            tableSinkSpec
                                    .getContextResolvedTable()
                                    .getIdentifier()
                                    .asSummaryString(),
                            sinkParallelism,
                            inputParallelism));
        }

        Object outputObject = null;
        if (runtimeProvider instanceof OutputFormatProvider) {
            outputObject = ((OutputFormatProvider) runtimeProvider).createOutputFormat();
        } else if (runtimeProvider instanceof SinkFunctionProvider) {
            outputObject = ((SinkFunctionProvider) runtimeProvider).createSinkFunction();
        } else if (runtimeProvider instanceof SinkV2Provider) {
            outputObject = ((SinkV2Provider) runtimeProvider).createSink();
        }

        Optional<LineageVertex> lineageVertexOpt =
                TableLineageUtils.extractLineageDataset(outputObject);

        // only add materialization if input has change
        final boolean needMaterialization = !inputInsertOnly && upsertMaterialize;

        Transformation<RowData> sinkTransform =
                applyConstraintValidations(inputTransform, config, physicalRowType);

        if (hasPk) {
            sinkTransform =
                    applyKeyBy(
                            config,
                            classLoader,
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            inputParallelism,
                            needMaterialization);
        }

        if (needMaterialization) {
            sinkTransform =
                    applyUpsertMaterialize(
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            config,
                            classLoader,
                            physicalRowType,
                            inputUpsertKey);
        }

        Optional<RowKind> targetRowKind = getTargetRowKind();
        if (targetRowKind.isPresent()) {
            sinkTransform = applyRowKindSetter(sinkTransform, targetRowKind.get(), config);
        }

        LineageDataset tableLineageDataset =
                TableLineageUtils.createTableLineageDataset(
                        tableSinkSpec.getContextResolvedTable(), lineageVertexOpt);

        TableSinkLineageVertex sinkLineageVertex =
                new TableSinkLineageVertexImpl(
                        Arrays.asList(tableLineageDataset),
                        TableLineageUtils.convert(inputChangelogMode));

        Transformation transformation =
                (Transformation<Object>)
                        applySinkProvider(
                                sinkTransform,
                                streamExecEnv,
                                runtimeProvider,
                                rowtimeFieldIndex,
                                sinkParallelism,
                                config,
                                classLoader);

        if (transformation instanceof TransformationWithLineage) {
            ((TransformationWithLineage<Object>) transformation)
                    .setLineageVertex(sinkLineageVertex);
        }
        return transformation;
    }

    /**
     * Apply an operator to filter or report error to process not-null values for not-null fields.
     */
    private Transformation<RowData> applyConstraintValidations(
            Transformation<RowData> inputTransform,
            ExecNodeConfig config,
            RowType physicalRowType) {
        final Optional<ConstraintEnforcerExecutor> enforcerExecutor =
                ConstraintEnforcerExecutor.create(
                        physicalRowType,
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER),
                        config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER),
                        config.get(
                                ExecutionConfigOptions.TABLE_EXEC_SINK_NESTED_CONSTRAINT_ENFORCER));

        return enforcerExecutor
                .map(
                        executor -> {
                            final String operatorName =
                                    "ConstraintEnforcer["
                                            + Arrays.stream(executor.getConstraints())
                                                    .map(Objects::toString)
                                                    .collect(Collectors.joining(", "))
                                            + "]";

                            return (Transformation<RowData>)
                                    ExecNodeUtil.createOneInputTransformation(
                                            inputTransform,
                                            createTransformationMeta(
                                                    CONSTRAINT_VALIDATOR_TRANSFORMATION,
                                                    operatorName,
                                                    "ConstraintEnforcer",
                                                    config),
                                            new ConstraintEnforcer(executor, operatorName),
                                            getInputTypeInfo(),
                                            inputTransform.getParallelism(),
                                            false);
                        })
                .orElse(inputTransform);
    }

    /**
     * Returns the parallelism of sink operator, it assumes the sink runtime provider implements
     * {@link ParallelismProvider}. It returns parallelism defined in {@link ParallelismProvider} if
     * the parallelism is provided, otherwise it uses parallelism of input transformation.
     */
    private int deriveSinkParallelism(
            Transformation<RowData> inputTransform, SinkRuntimeProvider runtimeProvider) {
        final int inputParallelism = inputTransform.getParallelism();
        if (isParallelismConfigured(runtimeProvider)) {
            int sinkParallelism = ((ParallelismProvider) runtimeProvider).getParallelism().get();
            if (sinkParallelism <= 0) {
                throw new TableException(
                        String.format(
                                "Invalid configured parallelism %s for table '%s'.",
                                sinkParallelism,
                                tableSinkSpec
                                        .getContextResolvedTable()
                                        .getIdentifier()
                                        .asSummaryString()));
            }
            return sinkParallelism;
        } else {
            return inputParallelism;
        }
    }

    private boolean isParallelismConfigured(DynamicTableSink.SinkRuntimeProvider runtimeProvider) {
        return runtimeProvider instanceof ParallelismProvider
                && ((ParallelismProvider) runtimeProvider).getParallelism().isPresent();
    }

    /**
     * Apply a primary key partition transformation to guarantee the strict ordering of changelog
     * messages.
     */
    private Transformation<RowData> applyKeyBy(
            ExecNodeConfig config,
            ClassLoader classLoader,
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            int inputParallelism,
            boolean needMaterialize) {
        final ExecutionConfigOptions.SinkKeyedShuffle sinkShuffleByPk =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE);
        boolean sinkKeyBy = false;
        switch (sinkShuffleByPk) {
            case NONE:
                break;
            case AUTO:
                // should cover both insert-only and changelog input
                sinkKeyBy = sinkParallelism != inputParallelism && sinkParallelism != 1;
                break;
            case FORCE:
                // sink single parallelism has no problem (because none partitioner will cause worse
                // disorder)
                sinkKeyBy = sinkParallelism != 1;
                break;
        }
        if (!sinkKeyBy && !needMaterialize) {
            return inputTransform;
        }

        final RowDataKeySelector selector =
                KeySelectorUtil.getRowDataSelector(classLoader, primaryKeys, getInputTypeInfo());
        final KeyGroupStreamPartitioner<RowData, RowData> partitioner =
                new KeyGroupStreamPartitioner<>(
                        selector, KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        Transformation<RowData> partitionedTransform =
                new PartitionTransformation<>(inputTransform, partitioner);
        createTransformationMeta(PARTITIONER_TRANSFORMATION, "Partitioner", "Partitioner", config)
                .fill(partitionedTransform);
        partitionedTransform.setParallelism(sinkParallelism, sinkParallelismConfigured);
        return partitionedTransform;
    }

    protected abstract Transformation<RowData> applyUpsertMaterialize(
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader,
            RowType physicalRowType,
            int[] inputUpsertKey);

    private Transformation<RowData> applyRowKindSetter(
            Transformation<RowData> inputTransform, RowKind rowKind, ExecNodeConfig config) {
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(
                        ROW_KIND_SETTER,
                        String.format("RowKindSetter(TargetRowKind=[%s])", rowKind),
                        "RowKindSetter",
                        config),
                new RowKindSetter(rowKind),
                inputTransform.getOutputType(),
                inputTransform.getParallelism(),
                false);
    }

    private Transformation<?> applySinkProvider(
            Transformation<RowData> inputTransform,
            StreamExecutionEnvironment env,
            SinkRuntimeProvider runtimeProvider,
            int rowtimeFieldIndex,
            int sinkParallelism,
            ExecNodeConfig config,
            ClassLoader classLoader) {
        try (TemporaryClassLoaderContext ignored = TemporaryClassLoaderContext.of(classLoader)) {

            TransformationMetadata sinkMeta = createTransformationMeta(SINK_TRANSFORMATION, config);
            if (runtimeProvider instanceof DataStreamSinkProvider) {
                Transformation<RowData> sinkTransformation =
                        applyRowtimeTransformation(
                                inputTransform, rowtimeFieldIndex, sinkParallelism, config);
                final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
                final DataStreamSinkProvider provider = (DataStreamSinkProvider) runtimeProvider;
                return provider.consumeDataStream(createProviderContext(config), dataStream)
                        .getTransformation();
            } else if (runtimeProvider instanceof TransformationSinkProvider) {
                final TransformationSinkProvider provider =
                        (TransformationSinkProvider) runtimeProvider;
                return provider.createTransformation(
                        new TransformationSinkProvider.Context() {
                            @Override
                            public Transformation<RowData> getInputTransformation() {
                                return inputTransform;
                            }

                            @Override
                            public int getRowtimeIndex() {
                                return rowtimeFieldIndex;
                            }

                            @Override
                            public Optional<String> generateUid(String name) {
                                return createProviderContext(config).generateUid(name);
                            }
                        });
            } else if (runtimeProvider instanceof SinkFunctionProvider) {
                final SinkFunction<RowData> sinkFunction =
                        ((SinkFunctionProvider) runtimeProvider).createSinkFunction();
                return createSinkFunctionTransformation(
                        sinkFunction,
                        env,
                        inputTransform,
                        rowtimeFieldIndex,
                        sinkMeta,
                        sinkParallelism);
            } else if (runtimeProvider instanceof OutputFormatProvider) {
                OutputFormat<RowData> outputFormat =
                        ((OutputFormatProvider) runtimeProvider).createOutputFormat();
                final SinkFunction<RowData> sinkFunction =
                        new OutputFormatSinkFunction<>(outputFormat);
                return createSinkFunctionTransformation(
                        sinkFunction,
                        env,
                        inputTransform,
                        rowtimeFieldIndex,
                        sinkMeta,
                        sinkParallelism);
            } else if (runtimeProvider instanceof SinkV2Provider) {
                Transformation<RowData> sinkTransformation =
                        applyRowtimeTransformation(
                                inputTransform, rowtimeFieldIndex, sinkParallelism, config);
                final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
                final Transformation<?> transformation =
                        DataStreamSink.forSink(
                                        dataStream,
                                        ((SinkV2Provider) runtimeProvider).createSink(),
                                        CustomSinkOperatorUidHashes.DEFAULT)
                                .getTransformation();
                transformation.setParallelism(sinkParallelism, sinkParallelismConfigured);
                sinkMeta.fill(transformation);
                return transformation;
            } else {
                throw new TableException("Unsupported sink runtime provider.");
            }
        }
    }

    private ProviderContext createProviderContext(ExecNodeConfig config) {
        return name -> {
            if (config.shouldSetUid()) {
                return Optional.of(createTransformationUid(name, config));
            }
            return Optional.empty();
        };
    }

    private Transformation<?> createSinkFunctionTransformation(
            SinkFunction<RowData> sinkFunction,
            StreamExecutionEnvironment env,
            Transformation<RowData> inputTransformation,
            int rowtimeFieldIndex,
            TransformationMetadata transformationMetadata,
            int sinkParallelism) {
        final SinkOperator operator = new SinkOperator(env.clean(sinkFunction), rowtimeFieldIndex);

        if (sinkFunction instanceof InputTypeConfigurable) {
            ((InputTypeConfigurable) sinkFunction)
                    .setInputType(getInputTypeInfo(), env.getConfig());
        }

        final Transformation<?> transformation =
                new LegacySinkTransformation<>(
                        inputTransformation,
                        transformationMetadata.getName(),
                        SimpleOperatorFactory.of(operator),
                        sinkParallelism,
                        sinkParallelismConfigured);
        transformationMetadata.fill(transformation);
        return transformation;
    }

    private Transformation<RowData> applyRowtimeTransformation(
            Transformation<RowData> inputTransform,
            int rowtimeFieldIndex,
            int sinkParallelism,
            ExecNodeConfig config) {
        // Don't apply the transformation/operator if there is no rowtimeFieldIndex
        if (rowtimeFieldIndex == -1) {
            return inputTransform;
        }
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationMeta(
                        TIMESTAMP_INSERTER_TRANSFORMATION,
                        String.format(
                                "StreamRecordTimestampInserter(rowtime field: %s)",
                                rowtimeFieldIndex),
                        "StreamRecordTimestampInserter",
                        config),
                new StreamRecordTimestampInserter(rowtimeFieldIndex),
                inputTransform.getOutputType(),
                sinkParallelism,
                sinkParallelismConfigured);
    }

    private InternalTypeInfo<RowData> getInputTypeInfo() {
        return InternalTypeInfo.of(getInputEdges().get(0).getOutputType());
    }

    protected int[] getPrimaryKeyIndices(RowType sinkRowType, ResolvedSchema schema) {
        return schema.getPrimaryKey()
                .map(k -> k.getColumns().stream().mapToInt(sinkRowType::getFieldIndex).toArray())
                .orElse(new int[0]);
    }

    protected RowType getPhysicalRowType(ResolvedSchema schema) {
        return (RowType) schema.toPhysicalRowDataType().getLogicalType();
    }

    /**
     * Get the target row-kind that the row data should change to, assuming the current row kind is
     * RowKind.INSERT. Return Optional.empty() if it doesn't need to change. Currently, it'll only
     * consider row-level delete/update.
     */
    private Optional<RowKind> getTargetRowKind() {
        if (tableSinkSpec.getSinkAbilities() != null) {
            for (SinkAbilitySpec sinkAbilitySpec : tableSinkSpec.getSinkAbilities()) {
                if (sinkAbilitySpec instanceof RowLevelDeleteSpec) {
                    RowLevelDeleteSpec deleteSpec = (RowLevelDeleteSpec) sinkAbilitySpec;
                    if (deleteSpec.getRowLevelDeleteMode()
                            == SupportsRowLevelDelete.RowLevelDeleteMode.DELETED_ROWS) {
                        return Optional.of(RowKind.DELETE);
                    }
                } else if (sinkAbilitySpec instanceof RowLevelUpdateSpec) {
                    RowLevelUpdateSpec updateSpec = (RowLevelUpdateSpec) sinkAbilitySpec;
                    if (updateSpec.getRowLevelUpdateMode()
                            == SupportsRowLevelUpdate.RowLevelUpdateMode.UPDATED_ROWS) {
                        return Optional.of(RowKind.UPDATE_AFTER);
                    }
                }
            }
        }
        return Optional.empty();
    }
}
