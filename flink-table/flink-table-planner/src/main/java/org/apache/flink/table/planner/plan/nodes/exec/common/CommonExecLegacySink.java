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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.SinkCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.sinks.DataStreamTableSink;
import org.apache.flink.table.planner.sinks.TableSinkUtils;
import org.apache.flink.table.runtime.operators.CodeGenOperatorFactory;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.sinks.AppendStreamTableSink;
import org.apache.flink.table.sinks.RetractStreamTableSink;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sinks.UpsertStreamTableSink;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Base {@link ExecNode} to write data into an external sink defined by a {@link TableSink}.
 *
 * @param <T> The return type of the {@link TableSink}.
 */
public abstract class CommonExecLegacySink<T> extends ExecNodeBase<T>
        implements MultipleTransformationTranslator<T> {
    protected final TableSink<T> tableSink;
    protected final @Nullable String[] upsertKeys;
    protected final boolean needRetraction;
    protected final boolean isStreaming;

    public CommonExecLegacySink(
            int id,
            ExecNodeContext context,
            ReadableConfig persistedConfig,
            TableSink<T> tableSink,
            @Nullable String[] upsertKeys,
            boolean needRetraction,
            boolean isStreaming,
            InputProperty inputProperty,
            LogicalType outputType,
            String description) {
        super(
                id,
                context,
                persistedConfig,
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.tableSink = tableSink;
        this.upsertKeys = upsertKeys;
        this.needRetraction = needRetraction;
        this.isStreaming = isStreaming;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<T> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        if (tableSink instanceof StreamTableSink) {
            final Transformation<T> transform;
            if (tableSink instanceof RetractStreamTableSink) {
                transform = translateToTransformation(planner, config, true);
            } else if (tableSink instanceof UpsertStreamTableSink) {
                UpsertStreamTableSink<T> upsertSink = (UpsertStreamTableSink<T>) tableSink;
                final boolean isAppendOnlyTable = !needRetraction;
                upsertSink.setIsAppendOnly(isAppendOnlyTable);
                if (upsertKeys != null) {
                    upsertSink.setKeyFields(upsertKeys);
                } else {
                    if (isAppendOnlyTable) {
                        upsertSink.setKeyFields(null);
                    } else {
                        throw new TableException(
                                "UpsertStreamTableSink requires that Table has a full primary keys if it is updated.");
                    }
                }

                transform = translateToTransformation(planner, config, true);
            } else if (tableSink instanceof AppendStreamTableSink) {
                // verify table is an insert-only (append-only) table
                if (needRetraction) {
                    throw new TableException(
                            "AppendStreamTableSink requires that Table has only insert changes.");
                }
                transform = translateToTransformation(planner, config, false);
            } else {
                if (isStreaming) {
                    throw new TableException(
                            "Stream Tables can only be emitted by AppendStreamTableSink, "
                                    + "RetractStreamTableSink, or UpsertStreamTableSink.");
                } else {
                    transform = translateToTransformation(planner, config, false);
                }
            }

            final DataStream<T> dataStream = new DataStream<T>(planner.getExecEnv(), transform);
            final DataStreamSink<T> dsSink =
                    (DataStreamSink<T>)
                            ((StreamTableSink<T>) tableSink).consumeDataStream(dataStream);
            if (dsSink == null) {
                throw new TableException(
                        String.format(
                                "The StreamTableSink#consumeDataStream(DataStream) must be implemented "
                                        + "and return the sink transformation DataStreamSink. "
                                        + "However, %s doesn't implement this method.",
                                tableSink.getClass().getCanonicalName()));
            }
            return dsSink.getLegacyTransformation();
        } else if (tableSink instanceof DataStreamTableSink) {
            // In case of table to DataStream through
            // StreamTableEnvironment#toAppendStream/toRetractStream,
            // we insert a DataStreamTableSink that wraps the given DataStream as a LogicalSink. It
            // is no real table sink, so we just need translate its input to Transformation.
            return translateToTransformation(
                    planner, config, ((DataStreamTableSink<T>) tableSink).withChangeFlag());
        } else {
            throw new TableException(
                    String.format(
                            "Only Support StreamTableSink! However %s is not a StreamTableSink.",
                            tableSink.getClass().getCanonicalName()));
        }
    }

    /** Check whether the given row type is legal and do some conversion if needed. */
    protected abstract RowType checkAndConvertInputTypeIfNeeded(RowType inputRowType);

    /**
     * Translates {@link TableSink} into a {@link Transformation}.
     *
     * @param withChangeFlag Set to true to emit records with change flags.
     * @return The {@link Transformation} that corresponds to the translated {@link TableSink}.
     */
    @SuppressWarnings("unchecked")
    private Transformation<T> translateToTransformation(
            PlannerBase planner, ExecNodeConfig config, boolean withChangeFlag) {
        // if no change flags are requested, verify table is an insert-only (append-only) table.
        if (!withChangeFlag && needRetraction) {
            throw new TableException(
                    "Table is not an append-only table. "
                            + "Use the toRetractStream() in order to handle add and retract messages.");
        }

        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        final RowType inputRowType = (RowType) inputEdge.getOutputType();
        final RowType convertedInputRowType = checkAndConvertInputTypeIfNeeded(inputRowType);
        final DataType resultDataType = tableSink.getConsumedDataType();
        if (CodeGenUtils.isInternalClass(resultDataType)) {
            return (Transformation<T>) inputTransform;
        } else {
            final int rowtimeIndex = getRowtimeIndex(inputRowType);

            final DataType physicalOutputType =
                    TableSinkUtils.inferSinkPhysicalDataType(
                            resultDataType, convertedInputRowType, withChangeFlag);

            final TypeInformation<T> outputTypeInfo =
                    SinkCodeGenerator.deriveSinkOutputTypeInfo(
                            tableSink, physicalOutputType, withChangeFlag);

            final CodeGenOperatorFactory<T> converterOperator =
                    SinkCodeGenerator.generateRowConverterOperator(
                            new CodeGeneratorContext(
                                    config, planner.getFlinkContext().getClassLoader()),
                            convertedInputRowType,
                            tableSink,
                            physicalOutputType,
                            withChangeFlag,
                            "SinkConversion",
                            rowtimeIndex);
            final String description =
                    "SinkConversion To " + resultDataType.getConversionClass().getSimpleName();
            return ExecNodeUtil.createOneInputTransformation(
                    inputTransform,
                    createFormattedTransformationName(description, "SinkConversion", config),
                    createFormattedTransformationDescription(description, config),
                    converterOperator,
                    outputTypeInfo,
                    inputTransform.getParallelism(),
                    false);
        }
    }

    private int getRowtimeIndex(RowType inputRowType) {
        int rowtimeIndex = -1;
        final List<Integer> rowtimeFieldIndices = new ArrayList<>();
        for (int i = 0; i < inputRowType.getFieldCount(); ++i) {
            if (TypeCheckUtils.isRowTime(inputRowType.getTypeAt(i))) {
                rowtimeFieldIndices.add(i);
            }
        }
        if (rowtimeFieldIndices.size() == 1) {
            rowtimeIndex = rowtimeFieldIndices.get(0);
        }
        return rowtimeIndex;
    }
}
