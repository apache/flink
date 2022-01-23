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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.DynamicTableSink.SinkRuntimeProvider;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator;
import org.apache.flink.table.planner.connectors.TransformationSinkProvider;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Base {@link ExecNode} to write data to an external sink defined by a {@link DynamicTableSink}.
 */
public abstract class CommonExecSink extends ExecNodeBase<Object>
        implements MultipleTransformationTranslator<Object> {

    public static final String FIELD_NAME_DYNAMIC_TABLE_SINK = "dynamicTableSink";

    @JsonProperty(FIELD_NAME_DYNAMIC_TABLE_SINK)
    protected final DynamicTableSinkSpec tableSinkSpec;

    @JsonIgnore private final ChangelogMode inputChangelogMode;
    @JsonIgnore private final boolean isBounded;

    protected CommonExecSink(
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode inputChangelogMode,
            boolean isBounded,
            int id,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(id, inputProperties, outputType, description);
        this.tableSinkSpec = tableSinkSpec;
        this.inputChangelogMode = inputChangelogMode;
        this.isBounded = isBounded;
    }

    @Override
    public String getSimplifiedName() {
        return tableSinkSpec.getObjectIdentifier().getObjectName();
    }

    public DynamicTableSinkSpec getTableSinkSpec() {
        return tableSinkSpec;
    }

    @SuppressWarnings("unchecked")
    protected Transformation<Object> createSinkTransformation(
            PlannerBase planner,
            Transformation<RowData> inputTransform,
            DynamicTableSink tableSink,
            int rowtimeFieldIndex,
            boolean upsertMaterialize) {
        final ResolvedSchema schema = tableSinkSpec.getCatalogTable().getResolvedSchema();
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
                            tableSinkSpec.getObjectIdentifier().asSummaryString(),
                            sinkParallelism,
                            inputParallelism));
        }

        // only add materialization if input has change
        final boolean needMaterialization = !inputInsertOnly && upsertMaterialize;

        Transformation<RowData> sinkTransform =
                applyConstraintValidations(
                        inputTransform, planner.getTableConfig(), physicalRowType);

        if (hasPk) {
            sinkTransform =
                    applyKeyBy(
                            planner.getTableConfig(),
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            inputParallelism,
                            inputInsertOnly,
                            needMaterialization);
        }

        if (needMaterialization) {
            sinkTransform =
                    applyUpsertMaterialize(
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            planner.getTableConfig(),
                            physicalRowType);
        }

        return (Transformation<Object>)
                applySinkProvider(
                        sinkTransform,
                        planner.getExecEnv(),
                        runtimeProvider,
                        rowtimeFieldIndex,
                        sinkParallelism,
                        planner.getTableConfig().getConfiguration());
    }

    /**
     * Apply an operator to filter or report error to process not-null values for not-null fields.
     */
    private Transformation<RowData> applyConstraintValidations(
            Transformation<RowData> inputTransform, TableConfig config, RowType physicalRowType) {
        final ConstraintEnforcer.Builder validatorBuilder = ConstraintEnforcer.newBuilder();
        final String[] fieldNames = physicalRowType.getFieldNames().toArray(new String[0]);

        // Build NOT NULL enforcer
        final int[] notNullFieldIndices = getNotNullFieldIndices(physicalRowType);
        if (notNullFieldIndices.length > 0) {
            final ExecutionConfigOptions.NotNullEnforcer notNullEnforcer =
                    config.getConfiguration()
                            .get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER);
            final List<String> notNullFieldNames =
                    Arrays.stream(notNullFieldIndices)
                            .mapToObj(idx -> fieldNames[idx])
                            .collect(Collectors.toList());

            validatorBuilder.addNotNullConstraint(
                    notNullEnforcer, notNullFieldIndices, notNullFieldNames, fieldNames);
        }

        final ExecutionConfigOptions.TypeLengthEnforcer typeLengthEnforcer =
                config.getConfiguration()
                        .get(ExecutionConfigOptions.TABLE_EXEC_SINK_TYPE_LENGTH_ENFORCER);

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
            final String operatorDesc =
                    getFormattedOperatorDescription(
                            constraintEnforcer.getOperatorName(), config.getConfiguration());
            final String operatorName =
                    getFormattedOperatorName(
                            constraintEnforcer.getOperatorName(),
                            "ConstraintEnforcer",
                            config.getConfiguration());
            return ExecNodeUtil.createOneInputTransformation(
                    inputTransform,
                    operatorName,
                    operatorDesc,
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
                                                        .getObjectIdentifier()
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
            TableConfig config,
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            int inputParallelism,
            boolean inputInsertOnly,
            boolean needMaterialize) {
        final ExecutionConfigOptions.SinkKeyedShuffle sinkShuffleByPk =
                config.getConfiguration().get(ExecutionConfigOptions.TABLE_EXEC_SINK_KEYED_SHUFFLE);
        boolean sinkKeyBy = false;
        switch (sinkShuffleByPk) {
            case NONE:
                break;
            case AUTO:
                sinkKeyBy = inputInsertOnly && sinkParallelism != inputParallelism;
                break;
            case FORCE:
                // single parallelism has no problem
                sinkKeyBy = sinkParallelism != 1 || inputParallelism != 1;
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
        partitionedTransform.setParallelism(sinkParallelism);
        return partitionedTransform;
    }

    private Transformation<RowData> applyUpsertMaterialize(
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            TableConfig tableConfig,
            RowType physicalRowType) {
        GeneratedRecordEqualiser equaliser =
                new EqualiserCodeGenerator(physicalRowType)
                        .generateRecordEqualiser("SinkMaterializeEqualiser");
        SinkUpsertMaterializer operator =
                new SinkUpsertMaterializer(
                        StateConfigUtil.createTtlConfig(
                                tableConfig.getIdleStateRetention().toMillis()),
                        InternalSerializers.create(physicalRowType),
                        equaliser);
        final String[] fieldNames = physicalRowType.getFieldNames().toArray(new String[0]);
        final List<String> pkFieldNames =
                Arrays.stream(primaryKeys)
                        .mapToObj(idx -> fieldNames[idx])
                        .collect(Collectors.toList());
        final String operatorDesc =
                getFormattedOperatorDescription(
                        String.format("SinkMaterializer(pk=[%s])", String.join(", ", pkFieldNames)),
                        tableConfig.getConfiguration());
        final String operatorName =
                getFormattedOperatorName(
                        operatorDesc, "SinkMaterializer", tableConfig.getConfiguration());
        OneInputTransformation<RowData, RowData> materializeTransform =
                ExecNodeUtil.createOneInputTransformation(
                        inputTransform,
                        operatorName,
                        operatorDesc,
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
            Configuration config) {
        String sinkName = getOperatorName(config);
        String sinkDescription = getOperatorDescription(config);
        if (runtimeProvider instanceof DataStreamSinkProvider) {
            Transformation<RowData> sinkTransformation =
                    applyRowtimeTransformation(
                            inputTransform, rowtimeFieldIndex, sinkParallelism, config);
            final DataStream<RowData> dataStream = new DataStream<>(env, sinkTransformation);
            final DataStreamSinkProvider provider = (DataStreamSinkProvider) runtimeProvider;
            return provider.consumeDataStream(dataStream).getTransformation();
        } else if (runtimeProvider instanceof TransformationSinkProvider) {
            final TransformationSinkProvider provider =
                    (TransformationSinkProvider) runtimeProvider;
            return provider.createTransformation(
                    TransformationSinkProvider.Context.of(inputTransform, rowtimeFieldIndex));
        } else if (runtimeProvider instanceof SinkFunctionProvider) {
            final SinkFunction<RowData> sinkFunction =
                    ((SinkFunctionProvider) runtimeProvider).createSinkFunction();
            return createSinkFunctionTransformation(
                    sinkFunction,
                    env,
                    inputTransform,
                    rowtimeFieldIndex,
                    sinkName,
                    sinkDescription,
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
                    sinkName,
                    sinkDescription,
                    sinkParallelism);
        } else if (runtimeProvider instanceof SinkProvider) {
            Transformation<?> transformation =
                    new SinkTransformation<>(
                            applyRowtimeTransformation(
                                    inputTransform, rowtimeFieldIndex, sinkParallelism, config),
                            ((SinkProvider) runtimeProvider).createSink(),
                            sinkName,
                            sinkParallelism);
            transformation.setDescription(sinkDescription);
            return transformation;
        } else {
            throw new TableException("Unsupported sink runtime provider.");
        }
    }

    private Transformation<?> createSinkFunctionTransformation(
            SinkFunction<RowData> sinkFunction,
            StreamExecutionEnvironment env,
            Transformation<RowData> inputTransformation,
            int rowtimeFieldIndex,
            String sinkName,
            String sinkDescription,
            int sinkParallelism) {
        final SinkOperator operator = new SinkOperator(env.clean(sinkFunction), rowtimeFieldIndex);

        if (sinkFunction instanceof InputTypeConfigurable) {
            ((InputTypeConfigurable) sinkFunction)
                    .setInputType(getInputTypeInfo(), env.getConfig());
        }

        final Transformation<?> transformation =
                new LegacySinkTransformation<>(
                        inputTransformation,
                        sinkName,
                        SimpleOperatorFactory.of(operator),
                        sinkParallelism);
        transformation.setDescription(sinkDescription);
        return transformation;
    }

    private Transformation<RowData> applyRowtimeTransformation(
            Transformation<RowData> inputTransform,
            int rowtimeFieldIndex,
            int sinkParallelism,
            Configuration config) {
        // Don't apply the transformation/operator if there is no rowtimeFieldIndex
        if (rowtimeFieldIndex == -1) {
            return inputTransform;
        }
        final String description =
                getFormattedOperatorDescription(
                        String.format(
                                "StreamRecordTimestampInserter(rowtime field: %s)",
                                rowtimeFieldIndex),
                        config);
        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                getFormattedOperatorName(description, "StreamRecordTimestampInserter", config),
                description,
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
