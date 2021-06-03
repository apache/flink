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
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.connectors.TransformationSinkProvider;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSinkSpec;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.sink.SinkNotNullEnforcer;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;

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
            int rowtimeFieldIndex) {
        final DynamicTableSink tableSink = tableSinkSpec.getTableSink();
        final DynamicTableSink.SinkRuntimeProvider runtimeProvider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(isBounded));
        final ResolvedSchema schema = tableSinkSpec.getCatalogTable().getResolvedSchema();
        final RowType physicalRowType = getPhysicalRowType(schema);
        inputTransform = applyNotNullEnforcer(tableConfig, physicalRowType, inputTransform);

        if (runtimeProvider instanceof DataStreamSinkProvider) {
            if (runtimeProvider instanceof ParallelismProvider) {
                throw new TableException(
                        "`DataStreamSinkProvider` is not allowed to work with"
                                + " `ParallelismProvider`, "
                                + "please see document of `ParallelismProvider`");
            }

            final DataStream<RowData> dataStream = new DataStream<>(env, inputTransform);
            final DataStreamSinkProvider provider = (DataStreamSinkProvider) runtimeProvider;
            return provider.consumeDataStream(dataStream).getTransformation();
        } else if (runtimeProvider instanceof TransformationSinkProvider) {
            final TransformationSinkProvider provider =
                    (TransformationSinkProvider) runtimeProvider;
            return (Transformation<Object>)
                    provider.createTransformation(
                            TransformationSinkProvider.Context.of(
                                    inputTransform, rowtimeFieldIndex));
        } else {
            checkArgument(
                    runtimeProvider instanceof ParallelismProvider,
                    "%s should implement ParallelismProvider interface.",
                    runtimeProvider.getClass().getName());
            final int inputParallelism = inputTransform.getParallelism();
            final int sinkParallelism =
                    deriveSinkParallelism((ParallelismProvider) runtimeProvider, inputParallelism);

            // apply keyBy partition transformation if needed
            inputTransform =
                    applyKeyByForDifferentParallelism(
                            physicalRowType,
                            schema.getPrimaryKey().orElse(null),
                            inputTransform,
                            inputParallelism,
                            sinkParallelism);

            final SinkFunction<RowData> sinkFunction;
            if (runtimeProvider instanceof SinkFunctionProvider) {
                sinkFunction = ((SinkFunctionProvider) runtimeProvider).createSinkFunction();
                return createSinkFunctionTransformation(
                        sinkFunction, env, inputTransform, rowtimeFieldIndex, sinkParallelism);

            } else if (runtimeProvider instanceof OutputFormatProvider) {
                OutputFormat<RowData> outputFormat =
                        ((OutputFormatProvider) runtimeProvider).createOutputFormat();
                sinkFunction = new OutputFormatSinkFunction<>(outputFormat);
                return createSinkFunctionTransformation(
                        sinkFunction, env, inputTransform, rowtimeFieldIndex, sinkParallelism);

            } else if (runtimeProvider instanceof SinkProvider) {
                return new SinkTransformation<>(
                        inputTransform,
                        ((SinkProvider) runtimeProvider).createSink(),
                        getDescription(),
                        sinkParallelism);

            } else {
                throw new TableException("This should not happen.");
            }
        }
    }

    /**
     * Apply an operator to filter or report error to process not-null values for not-null fields.
     */
    private Transformation<RowData> applyNotNullEnforcer(
            TableConfig config, RowType physicalRowType, Transformation<RowData> inputTransform) {
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
            ParallelismProvider parallelismProvider, int inputParallelism) {
        final Optional<Integer> parallelismOptional = parallelismProvider.getParallelism();
        if (parallelismOptional.isPresent()) {
            int sinkParallelism = parallelismOptional.get();
            if (sinkParallelism <= 0) {
                throw new TableException(
                        String.format(
                                "Table: %s configured sink parallelism: "
                                        + "%s should not be less than zero or equal to zero",
                                tableSinkSpec.getObjectIdentifier().asSummaryString(),
                                sinkParallelism));
            }
            return sinkParallelism;
        } else {
            // use input parallelism if not specified
            return inputParallelism;
        }
    }

    /**
     * Apply a keyBy partition transformation if the parallelism of sink operator and input operator
     * is different and sink changelog-mode is not insert-only. This is used to guarantee the strict
     * ordering of changelog messages.
     */
    private Transformation<RowData> applyKeyByForDifferentParallelism(
            RowType sinkRowType,
            @Nullable UniqueConstraint primaryKey,
            Transformation<RowData> inputTransform,
            int inputParallelism,
            int sinkParallelism) {
        final int[] primaryKeys = getPrimaryKeyIndices(sinkRowType, primaryKey);
        if (inputParallelism == sinkParallelism || changelogMode.containsOnly(RowKind.INSERT)) {
            // if the inputParallelism is equals to the parallelism or insert-only mode, do nothing.
            return inputTransform;
        } else if (primaryKeys.length == 0) {
            throw new TableException(
                    String.format(
                            "Table: %s configured sink parallelism is: %s, while the input parallelism is: "
                                    + "%s. Since configured parallelism is different from input parallelism and the changelog mode "
                                    + "contains [%s], which is not INSERT_ONLY mode, primary key is required but no primary key is found",
                            tableSinkSpec.getObjectIdentifier().asSummaryString(),
                            sinkParallelism,
                            inputParallelism,
                            changelogMode.getContainedKinds().stream()
                                    .map(Enum::toString)
                                    .collect(Collectors.joining(","))));
        } else {
            // keyBy before sink
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
    }

    private int[] getPrimaryKeyIndices(RowType sinkRowType, @Nullable UniqueConstraint primaryKey) {
        if (primaryKey == null) {
            return new int[0];
        }
        return primaryKey.getColumns().stream().mapToInt(sinkRowType::getFieldIndex).toArray();
    }

    private Transformation<Object> createSinkFunctionTransformation(
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

    private InternalTypeInfo<RowData> getInputTypeInfo() {
        return InternalTypeInfo.of(getInputEdges().get(0).getOutputType());
    }

    private RowType getPhysicalRowType(ResolvedSchema schema) {
        return (RowType) schema.toPhysicalRowDataType().getLogicalType();
    }
}
