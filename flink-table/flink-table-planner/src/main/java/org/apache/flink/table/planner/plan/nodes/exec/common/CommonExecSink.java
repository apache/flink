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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
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
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.connector.sink.SinkV2Provider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.connectors.TransformationSinkProvider;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.stream.StreamExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.nodes.exec.utils.TransformationMetadata;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.sink.ConstraintEnforcer;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer;
import org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Base {@link ExecNode} to write data to an external sink defined by a {@link DynamicTableSink}.
 */
public abstract class CommonExecSink extends ExecNodeBase<Object>
        implements MultipleTransformationTranslator<Object> {

    public static final String CONSTRAINT_VALIDATOR_TRANSFORMATION = "constraint-validator";
    public static final String PARTITIONER_TRANSFORMATION = "partitioner";
    public static final String UPSERT_MATERIALIZE_TRANSFORMATION = "upsert-materialize";
    public static final String TIMESTAMP_INSERTER_TRANSFORMATION = "timestamp-inserter";
    public static final String SINK_TRANSFORMATION = "sink";

    public static final String FIELD_NAME_DYNAMIC_TABLE_SINK = "dynamicTableSink";

    @JsonProperty(FIELD_NAME_DYNAMIC_TABLE_SINK)
    protected final DynamicTableSinkSpec tableSinkSpec;

    private final ChangelogMode inputChangelogMode;
    private final boolean isBounded;

    protected CommonExecSink(
            int id,
            ExecNodeContext context,
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode inputChangelogMode,
            boolean isBounded,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(id, context, inputProperties, outputType, description);
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
            ReadableConfig config,
            Transformation<RowData> inputTransform,
            DynamicTableSink tableSink,
            int rowtimeFieldIndex,
            boolean upsertMaterialize) {
        final ResolvedSchema schema = tableSinkSpec.getContextResolvedTable().getResolvedSchema();
        final SinkRuntimeProvider runtimeProvider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(isBounded));
        final RowType physicalRowType = getPhysicalRowType(schema);
        final int[] primaryKeys = getPrimaryKeyIndices(physicalRowType, schema);
        final int sinkParallelism = deriveSinkParallelism(inputTransform, runtimeProvider);
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

        // only add materialization if input has change
        final boolean needMaterialization = !inputInsertOnly && upsertMaterialize;

        Transformation<RowData> sinkTransform =
                applyConstraintValidations(inputTransform, config, physicalRowType);

        if (hasPk) {
            sinkTransform =
                    applyKeyBy(
                            config,
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            inputParallelism,
                            needMaterialization);
        }

        if (needMaterialization) {
            sinkTransform =
                    applyUpsertMaterialize(
                            sinkTransform, primaryKeys, sinkParallelism, config, physicalRowType);
        }

        return (Transformation<Object>)
                applySinkProvider(
                        sinkTransform,
                        streamExecEnv,
                        runtimeProvider,
                        rowtimeFieldIndex,
                        sinkParallelism,
                        config);
    }

    /**
     * Apply an operator to filter or report error to process not-null values for not-null fields.
     */
    private Transformation<RowData> applyConstraintValidations(
            Transformation<RowData> inputTransform,
            ReadableConfig config,
            RowType physicalRowType) {
        final ConstraintEnforcer.Builder validatorBuilder = ConstraintEnforcer.newBuilder();
        final String[] fieldNames = physicalRowType.getFieldNames().toArray(new String[0]);

        // Build NOT NULL enforcer
        final int[] notNullFieldIndices = getNotNullFieldIndices(physicalRowType);
        if (notNullFieldIndices.length > 0) {
            final ExecutionConfigOptions.NotNullEnforcer notNullEnforcer =
                    config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER);
            final List<String> notNullFieldNames =
                    Arrays.stream(notNullFieldIndices)
                            .mapToObj(idx -> fieldNames[idx])
                            .collect(Collectors.toList());

            validatorBuilder.addNotNullConstraint(
                    notNullEnforcer, notNullFieldIndices, notNullFieldNames, fieldNames);
        }

        final ExecutionConfigOptions.TypeLengthEnforcer typeLengthEnforcer =
                config.get(ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER);

        // Build CHAR/VARCHAR length enforcer
        final List<ConstraintEnforcer.FieldInfo> charFieldInfo =
                getFieldInfoForLengthEnforcer(physicalRowType, LengthEnforcerType.CHAR);
        if (!charFieldInfo.isEmpty()) {
            final List<String> charFieldNames =
                    charFieldInfo.stream()
                            .map(cfi -> fieldNames[cfi.fieldIdx()])
                            .collect(Collectors.toList());

            validatorBuilder.addCharLengthConstraint(
                    typeLengthEnforcer, charFieldInfo, charFieldNames, fieldNames);
        }

        // Build BINARY/VARBINARY length enforcer
        final List<ConstraintEnforcer.FieldInfo> binaryFieldInfo =
                getFieldInfoForLengthEnforcer(physicalRowType, LengthEnforcerType.BINARY);
        if (!binaryFieldInfo.isEmpty()) {
            final List<String> binaryFieldNames =
                    binaryFieldInfo.stream()
                            .map(cfi -> fieldNames[cfi.fieldIdx()])
                            .collect(Collectors.toList());

            validatorBuilder.addBinaryLengthConstraint(
                    typeLengthEnforcer, binaryFieldInfo, binaryFieldNames, fieldNames);
        }

        ConstraintEnforcer constraintEnforcer = validatorBuilder.build();
        if (constraintEnforcer != null) {
            return ExecNodeUtil.createOneInputTransformation(
                    inputTransform,
                    createTransformationMeta(
                            CONSTRAINT_VALIDATOR_TRANSFORMATION,
                            constraintEnforcer.getOperatorName(),
                            "ConstraintEnforcer",
                            config),
                    constraintEnforcer,
                    getInputTypeInfo(),
                    inputTransform.getParallelism());
        } else {
            // there are no not-null fields, just skip adding the enforcer operator
            return inputTransform;
        }
    }

    private int[] getNotNullFieldIndices(RowType physicalType) {
        return IntStream.range(0, physicalType.getFieldCount())
                .filter(pos -> !physicalType.getTypeAt(pos).isNullable())
                .toArray();
    }

    /**
     * Returns a List of {@link ConstraintEnforcer.FieldInfo}, each containing the info needed to
     * determine whether a string or binary value needs trimming and/or padding.
     */
    private List<ConstraintEnforcer.FieldInfo> getFieldInfoForLengthEnforcer(
            RowType physicalType, LengthEnforcerType enforcerType) {
        LogicalTypeRoot staticType = null;
        LogicalTypeRoot variableType = null;
        int maxLength = 0;
        switch (enforcerType) {
            case CHAR:
                staticType = LogicalTypeRoot.CHAR;
                variableType = LogicalTypeRoot.VARCHAR;
                maxLength = CharType.MAX_LENGTH;
                break;
            case BINARY:
                staticType = LogicalTypeRoot.BINARY;
                variableType = LogicalTypeRoot.VARBINARY;
                maxLength = BinaryType.MAX_LENGTH;
        }
        final List<ConstraintEnforcer.FieldInfo> fieldsAndLengths = new ArrayList<>();
        for (int i = 0; i < physicalType.getFieldCount(); i++) {
            LogicalType type = physicalType.getTypeAt(i);
            boolean isStatic = type.is(staticType);
            // Should trim and possibly pad
            if ((isStatic && (LogicalTypeChecks.getLength(type) < maxLength))
                    || (type.is(variableType) && (LogicalTypeChecks.getLength(type) < maxLength))) {
                fieldsAndLengths.add(
                        new ConstraintEnforcer.FieldInfo(
                                i, LogicalTypeChecks.getLength(type), isStatic));
            } else if (isStatic) { // Should pad
                fieldsAndLengths.add(new ConstraintEnforcer.FieldInfo(i, null, isStatic));
            }
        }
        return fieldsAndLengths;
    }

    /**
     * Returns the parallelism of sink operator, it assumes the sink runtime provider implements
     * {@link ParallelismProvider}. It returns parallelism defined in {@link ParallelismProvider} if
     * the parallelism is provided, otherwise it uses parallelism of input transformation.
     */
    private int deriveSinkParallelism(
            Transformation<RowData> inputTransform, SinkRuntimeProvider runtimeProvider) {
        final int inputParallelism = inputTransform.getParallelism();
        if (!(runtimeProvider instanceof ParallelismProvider)) {
            return inputParallelism;
        }
        final ParallelismProvider parallelismProvider = (ParallelismProvider) runtimeProvider;
        return parallelismProvider
                .getParallelism()
                .map(
                        sinkParallelism -> {
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
                        })
                .orElse(inputParallelism);
    }

    /**
     * Apply a primary key partition transformation to guarantee the strict ordering of changelog
     * messages.
     */
    private Transformation<RowData> applyKeyBy(
            ReadableConfig config,
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
                KeySelectorUtil.getRowDataSelector(primaryKeys, getInputTypeInfo());
        final KeyGroupStreamPartitioner<RowData, RowData> partitioner =
                new KeyGroupStreamPartitioner<>(
                        selector, KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
        Transformation<RowData> partitionedTransform =
                new PartitionTransformation<>(inputTransform, partitioner);
        createTransformationMeta(PARTITIONER_TRANSFORMATION, "Partitioner", "Partitioner", config)
                .fill(partitionedTransform);
        partitionedTransform.setParallelism(sinkParallelism);
        return partitionedTransform;
    }

    private Transformation<RowData> applyUpsertMaterialize(
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            ReadableConfig config,
            RowType physicalRowType) {
        GeneratedRecordEqualiser equaliser =
                new EqualiserCodeGenerator(physicalRowType)
                        .generateRecordEqualiser("SinkMaterializeEqualiser");
        SinkUpsertMaterializer operator =
                new SinkUpsertMaterializer(
                        StateConfigUtil.createTtlConfig(
                                config.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis()),
                        InternalSerializers.create(physicalRowType),
                        equaliser);
        final String[] fieldNames = physicalRowType.getFieldNames().toArray(new String[0]);
        final List<String> pkFieldNames =
                Arrays.stream(primaryKeys)
                        .mapToObj(idx -> fieldNames[idx])
                        .collect(Collectors.toList());

        OneInputTransformation<RowData, RowData> materializeTransform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        createTransformationMeta(
                                UPSERT_MATERIALIZE_TRANSFORMATION,
                                String.format(
                                        "SinkMaterializer(pk=[%s])",
                                        String.join(", ", pkFieldNames)),
                                "SinkMaterializer",
                                config),
                        operator,
                        inputTransform.getOutputType(),
                        sinkParallelism);
        RowDataKeySelector keySelector =
                KeySelectorUtil.getRowDataSelector(
                        primaryKeys, InternalTypeInfo.of(physicalRowType));
        materializeTransform.setStateKeySelector(keySelector);
        materializeTransform.setStateKeyType(keySelector.getProducedType());
        return materializeTransform;
    }

    private Transformation<?> applySinkProvider(
            Transformation<RowData> inputTransform,
            StreamExecutionEnvironment env,
            SinkRuntimeProvider runtimeProvider,
            int rowtimeFieldIndex,
            int sinkParallelism,
            ReadableConfig config) {
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
            final SinkFunction<RowData> sinkFunction = new OutputFormatSinkFunction<>(outputFormat);
            return createSinkFunctionTransformation(
                    sinkFunction,
                    env,
                    inputTransform,
                    rowtimeFieldIndex,
                    sinkMeta,
                    sinkParallelism);
        } else if (runtimeProvider instanceof SinkProvider) {
            Transformation<RowData> sinkTransformation =
                    applyRowtimeTransformation(
                            inputTransform, rowtimeFieldIndex, sinkParallelism, config);
            final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
            final Transformation<?> transformation =
                    DataStreamSink.forSinkV1(
                                    dataStream, ((SinkProvider) runtimeProvider).createSink())
                            .getTransformation();
            transformation.setParallelism(sinkParallelism);
            sinkMeta.fill(transformation);
            return transformation;
        } else if (runtimeProvider instanceof SinkV2Provider) {
            Transformation<RowData> sinkTransformation =
                    applyRowtimeTransformation(
                            inputTransform, rowtimeFieldIndex, sinkParallelism, config);
            final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
            final Transformation<?> transformation =
                    DataStreamSink.forSink(
                                    dataStream, ((SinkV2Provider) runtimeProvider).createSink())
                            .getTransformation();
            transformation.setParallelism(sinkParallelism);
            sinkMeta.fill(transformation);
            return transformation;
        } else {
            throw new TableException("Unsupported sink runtime provider.");
        }
    }

    private ProviderContext createProviderContext(ReadableConfig config) {
        return name -> {
            if (this instanceof StreamExecNode
                    && !config.get(ExecutionConfigOptions.TABLE_EXEC_LEGACY_TRANSFORMATION_UIDS)) {
                return Optional.of(createTransformationUid(name));
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
                        sinkParallelism);
        transformationMetadata.fill(transformation);
        return transformation;
    }

    private Transformation<RowData> applyRowtimeTransformation(
            Transformation<RowData> inputTransform,
            int rowtimeFieldIndex,
            int sinkParallelism,
            ReadableConfig config) {
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
                sinkParallelism);
    }

    private InternalTypeInfo<RowData> getInputTypeInfo() {
        return InternalTypeInfo.of(getInputEdges().get(0).getOutputType());
    }

    private int[] getPrimaryKeyIndices(RowType sinkRowType, ResolvedSchema schema) {
        return schema.getPrimaryKey()
                .map(k -> k.getColumns().stream().mapToInt(sinkRowType::getFieldIndex).toArray())
                .orElse(new int[0]);
    }

    private RowType getPhysicalRowType(ResolvedSchema schema) {
        return (RowType) schema.toPhysicalRowDataType().getLogicalType();
    }

    private enum LengthEnforcerType {
        CHAR,
        BINARY
    }
}
