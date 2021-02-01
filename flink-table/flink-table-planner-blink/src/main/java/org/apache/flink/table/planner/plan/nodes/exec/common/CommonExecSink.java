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
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.ParallelismProvider;
import org.apache.flink.table.connector.sink.DataStreamSinkProvider;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.OutputFormatProvider;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil;
import org.apache.flink.table.planner.sinks.TableSinkUtils;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.keyselector.RowDataKeySelector;
import org.apache.flink.table.runtime.operators.sink.SinkNotNullEnforcer;
import org.apache.flink.table.runtime.operators.sink.SinkOperator;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Base {@link ExecNode} to write data to an external sink defined by a {@link DynamicTableSink}.
 */
public abstract class CommonExecSink extends ExecNodeBase<Object> {
    protected final List<String> qualifiedName;
    private final TableSchema tableSchema;
    private final DynamicTableSink tableSink;
    private final ChangelogMode changelogMode;
    private final boolean isBounded;

    public CommonExecSink(
            List<String> qualifiedName,
            TableSchema tableSchema,
            DynamicTableSink tableSink,
            ChangelogMode changelogMode,
            boolean isBounded,
            InputProperty inputProperty,
            LogicalType outputType,
            String description) {
        super(Collections.singletonList(inputProperty), outputType, description);
        this.tableSink = tableSink;
        this.qualifiedName = qualifiedName;
        this.tableSchema = tableSchema;
        this.changelogMode = changelogMode;
        this.isBounded = isBounded;
    }

    protected Transformation<Object> createSinkTransformation(
            StreamExecutionEnvironment env,
            TableConfig tableConfig,
            Transformation<RowData> inputTransform,
            int rowtimeFieldIndex) {
        final DynamicTableSink.SinkRuntimeProvider runtimeProvider =
                tableSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(isBounded));

        final ExecutionConfigOptions.NotNullEnforcer notNullEnforcer =
                tableConfig
                        .getConfiguration()
                        .get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER);
        final int[] notNullFieldIndices = TableSinkUtils.getNotNullFieldIndices(tableSchema);
        final String[] fieldNames =
                ((RowType) tableSchema.toPhysicalRowDataType().getLogicalType())
                        .getFieldNames()
                        .toArray(new String[0]);
        final SinkNotNullEnforcer enforcer =
                new SinkNotNullEnforcer(notNullEnforcer, notNullFieldIndices, fieldNames);
        final InternalTypeInfo<RowData> inputTypeInfo =
                InternalTypeInfo.of(getInputEdges().get(0).getOutputType());

        if (runtimeProvider instanceof DataStreamSinkProvider) {
            if (runtimeProvider instanceof ParallelismProvider) {
                throw new TableException(
                        "`DataStreamSinkProvider` is not allowed to work with"
                                + " `ParallelismProvider`, "
                                + "please see document of `ParallelismProvider`");
            } else {
                final DataStream<RowData> dataStream =
                        new DataStream<>(env, inputTransform).filter(enforcer);
                final DataStreamSinkProvider provider = (DataStreamSinkProvider) runtimeProvider;
                return provider.consumeDataStream(dataStream).getTransformation();
            }
        } else {
            Preconditions.checkArgument(
                    runtimeProvider instanceof ParallelismProvider,
                    "runtimeProvider with `ParallelismProvider` implementation is required");

            final SinkFunction<RowData> sinkFunction;
            if (runtimeProvider instanceof SinkFunctionProvider) {
                sinkFunction = ((SinkFunctionProvider) runtimeProvider).createSinkFunction();
            } else if (runtimeProvider instanceof OutputFormatProvider) {
                OutputFormat<RowData> outputFormat =
                        ((OutputFormatProvider) runtimeProvider).createOutputFormat();
                sinkFunction = new OutputFormatSinkFunction<>(outputFormat);
            } else {
                throw new TableException("This should not happen.");
            }

            if (sinkFunction instanceof InputTypeConfigurable) {
                ((InputTypeConfigurable) sinkFunction).setInputType(inputTypeInfo, env.getConfig());
            }

            final SinkOperator operator =
                    new SinkOperator(env.clean(sinkFunction), rowtimeFieldIndex, enforcer);

            final int inputParallelism = inputTransform.getParallelism();
            final int parallelism;
            final Optional<Integer> parallelismOptional =
                    ((ParallelismProvider) runtimeProvider).getParallelism();
            if (parallelismOptional.isPresent()) {
                parallelism = parallelismOptional.get();
                if (parallelism <= 0) {
                    throw new TableException(
                            String.format(
                                    "Table: %s configured sink parallelism: "
                                            + "%s should not be less than zero or equal to zero",
                                    String.join(".", qualifiedName), parallelism));
                }
            } else {
                parallelism = inputParallelism;
            }

            final int[] primaryKeys = TableSchemaUtils.getPrimaryKeyIndices(tableSchema);
            final Transformation<RowData> finalInputTransform;
            if (inputParallelism == parallelism || changelogMode.containsOnly(RowKind.INSERT)) {
                // if the inputParallelism is equals to the parallelism or insert-only mode, do
                // nothing.
                finalInputTransform = inputTransform;
            } else if (primaryKeys.length == 0) {
                throw new TableException(
                        String.format(
                                "Table: %s configured sink parallelism is: %s, while the input parallelism is: "
                                        + "%s. Since configured parallelism is different from input parallelism and the changelog mode "
                                        + "contains [%s], which is not INSERT_ONLY mode, primary key is required but no primary key is found",
                                String.join(".", qualifiedName),
                                parallelism,
                                inputParallelism,
                                changelogMode.getContainedKinds().stream()
                                        .map(Enum::toString)
                                        .collect(Collectors.joining(","))));
            } else {
                // key by before sink
                final RowDataKeySelector selector =
                        KeySelectorUtil.getRowDataSelector(primaryKeys, inputTypeInfo);
                final KeyGroupStreamPartitioner<RowData, RowData> partitioner =
                        new KeyGroupStreamPartitioner<>(
                                selector,
                                KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM);
                finalInputTransform = new PartitionTransformation<>(inputTransform, partitioner);
                finalInputTransform.setParallelism(parallelism);
            }
            return new LegacySinkTransformation<>(
                    finalInputTransform,
                    getDescription(),
                    SimpleOperatorFactory.of(operator),
                    parallelism);
        }
    }
}
