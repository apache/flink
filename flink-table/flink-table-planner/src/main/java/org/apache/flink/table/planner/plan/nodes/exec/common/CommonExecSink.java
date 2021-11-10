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
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamFilter;
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
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.generated.GeneratedRecordEqualiser;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.sink.SinkNotNullEnforcer;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.table.runtime.operators.sink.SinkUpsertMaterializer;
import org.apache.flink.table.runtime.operators.sink.StreamRecordTimestampInserter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.util.StateConfigUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

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

    @JsonIgnore private final ChangelogMode changelogMode;
    @JsonIgnore private final boolean isBounded;

    protected CommonExecSink(
            DynamicTableSinkSpec tableSinkSpec,
            ChangelogMode changelogMode,
            boolean isBounded,
            int id,
            List<InputProperty> inputProperties,
            LogicalType outputType,
            String description) {
        super(id, inputProperties, outputType, description);
        this.tableSinkSpec = tableSinkSpec;
        this.changelogMode = changelogMode;
        this.isBounded = isBounded;
    }

    public DynamicTableSinkSpec getTableSinkSpec() {
        return tableSinkSpec;
    }

    @SuppressWarnings("unchecked")
    protected Transformation<Object> createSinkTransformation(
            StreamExecutionEnvironment env,
            TableConfig tableConfig,
            Transformation<RowData> inputTransform,
            int rowtimeFieldIndex,
            boolean upsertMaterialize) {
        final DynamicTableSink tableSink = tableSinkSpec.getTableSink();
        final ResolvedSchema schema = tableSinkSpec.getCatalogTable().getResolvedSchema();

        final SinkRuntimeProvider runtimeProvider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(isBounded));

        final RowType physicalRowType = getPhysicalRowType(schema);

        final int[] primaryKeys = getPrimaryKeyIndices(physicalRowType, schema);

        final int sinkParallelism = deriveSinkParallelism(inputTransform, runtimeProvider);

        Transformation<RowData> sinkTransform =
                applyNotNullEnforcer(inputTransform, tableConfig, physicalRowType);

        sinkTransform = applyKeyBy(sinkTransform, primaryKeys, sinkParallelism, upsertMaterialize);

        if (upsertMaterialize) {
            sinkTransform =
                    applyUpsertMaterialize(
                            sinkTransform,
                            primaryKeys,
                            sinkParallelism,
                            tableConfig,
                            physicalRowType);
        }

        return (Transformation<Object>)
                applySinkProvider(
                        sinkTransform, env, runtimeProvider, rowtimeFieldIndex, sinkParallelism);
    }

    /**
     * Apply an operator to filter or report error to process not-null values for not-null fields.
     */
    private Transformation<RowData> applyNotNullEnforcer(
            Transformation<RowData> inputTransform, TableConfig config, RowType physicalRowType) {
        final ExecutionConfigOptions.NotNullEnforcer notNullEnforcer =
                config.getConfiguration()
                        .get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER);
        final int[] notNullFieldIndices = getNotNullFieldIndices(physicalRowType);
        final String[] fieldNames = physicalRowType.getFieldNames().toArray(new String[0]);

        if (notNullFieldIndices.length > 0) {
            final SinkNotNullEnforcer enforcer =
                    new SinkNotNullEnforcer(notNullEnforcer, notNullFieldIndices, fieldNames);
            final List<String> notNullFieldNames =
                    Arrays.stream(notNullFieldIndices)
                            .mapToObj(idx -> fieldNames[idx])
                            .collect(Collectors.toList());
            final String operatorName =
                    String.format(
                            "NotNullEnforcer(fields=[%s])", String.join(", ", notNullFieldNames));
            return new OneInputTransformation<>(
                    inputTransform,
                    operatorName,
                    new StreamFilter<>(enforcer),
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
            Transformation<RowData> inputTransform,
            int[] primaryKeys,
            int sinkParallelism,
            boolean upsertMaterialize) {
        final int inputParallelism = inputTransform.getParallelism();
        if ((inputParallelism == sinkParallelism || changelogMode.containsOnly(RowKind.INSERT))
                && !upsertMaterialize) {
            return inputTransform;
        }
        if (primaryKeys.length == 0) {
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
        OneInputTransformation<RowData, RowData> materializeTransform =
                new OneInputTransformation<>(
                        inputTransform,
                        "SinkMaterializer",
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
            int sinkParallelism) {
        if (runtimeProvider instanceof DataStreamSinkProvider) {
            Transformation<RowData> sinkTransformation =
                    applyRowtimeTransformation(inputTransform, rowtimeFieldIndex, sinkParallelism);
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
                    sinkFunction, env, inputTransform, rowtimeFieldIndex, sinkParallelism);
        } else if (runtimeProvider instanceof OutputFormatProvider) {
            OutputFormat<RowData> outputFormat =
                    ((OutputFormatProvider) runtimeProvider).createOutputFormat();
            final SinkFunction<RowData> sinkFunction = new OutputFormatSinkFunction<>(outputFormat);
            return createSinkFunctionTransformation(
                    sinkFunction, env, inputTransform, rowtimeFieldIndex, sinkParallelism);
        } else if (runtimeProvider instanceof SinkProvider) {
            return new SinkTransformation<>(
                    applyRowtimeTransformation(inputTransform, rowtimeFieldIndex, sinkParallelism),
                    ((SinkProvider) runtimeProvider).createSink(),
                    getDescription(),
                    sinkParallelism);
        } else {
            throw new TableException("Unsupported sink runtime provider.");
        }
    }

    private Transformation<?> createSinkFunctionTransformation(
            SinkFunction<RowData> sinkFunction,
            StreamExecutionEnvironment env,
            Transformation<RowData> inputTransformation,
            int rowtimeFieldIndex,
            int sinkParallelism) {
        final SinkOperator operator = new SinkOperator(env.clean(sinkFunction), rowtimeFieldIndex);

        if (sinkFunction instanceof InputTypeConfigurable) {
            ((InputTypeConfigurable) sinkFunction)
                    .setInputType(getInputTypeInfo(), env.getConfig());
        }

        return new LegacySinkTransformation<>(
                inputTransformation,
                getDescription(),
                SimpleOperatorFactory.of(operator),
                sinkParallelism);
    }

    private Transformation<RowData> applyRowtimeTransformation(
            Transformation<RowData> inputTransform, int rowtimeFieldIndex, int sinkParallelism) {
        // Don't apply the transformation/operator if there is no rowtimeFieldIndex
        if (rowtimeFieldIndex == -1) {
            return inputTransform;
        }
        return new OneInputTransformation<>(
                inputTransform,
                String.format(
                        "StreamRecordTimestampInserter(rowtime field: %s)", rowtimeFieldIndex),
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
}
