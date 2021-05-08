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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.functions.sql.StreamRecordTimestampSqlFunction;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.utils.ScanUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.typeutils.TypeCheckUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.calcite.rex.RexNode;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;
import static org.apache.flink.table.typeutils.TimeIndicatorTypeInfo.ROWTIME_STREAM_MARKER;

/** Stream {@link ExecNode} to connect a given {@link DataStream} and consume data from it. */
public class StreamExecDataStreamScan extends ExecNodeBase<RowData>
        implements StreamExecNode<RowData>, MultipleTransformationTranslator<RowData> {
    private final DataStream<?> dataStream;
    private final DataType sourceType;
    private final int[] fieldIndexes;
    private final String[] fieldNames;
    private final List<String> qualifiedName;

    public StreamExecDataStreamScan(
            DataStream<?> dataStream,
            DataType sourceType,
            int[] fieldIndexes,
            String[] fieldNames,
            List<String> qualifiedName,
            RowType outputType,
            String description) {
        super(Collections.emptyList(), outputType, description);
        this.dataStream = dataStream;
        this.sourceType = sourceType;
        this.fieldIndexes = fieldIndexes;
        this.fieldNames = fieldNames;
        this.qualifiedName = qualifiedName;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final Transformation<?> sourceTransform = dataStream.getTransformation();
        final Optional<RexNode> rowtimeExpr = getRowtimeExpression(planner.getRelBuilder());

        final Transformation<RowData> transformation;
        // when there is row time extraction expression, we need internal conversion
        // when the physical type of the input date stream is not RowData, we need internal
        // conversion.
        if (rowtimeExpr.isPresent() || ScanUtil.needsConversion(sourceType)) {
            final String extractElement, resetElement;
            if (ScanUtil.hasTimeAttributeField(fieldIndexes)) {
                String elementTerm = OperatorCodeGenerator.ELEMENT();
                extractElement = String.format("ctx.%s = %s;", elementTerm, elementTerm);
                resetElement = String.format("ctx.%s = null;", elementTerm);
            } else {
                extractElement = "";
                resetElement = "";
            }
            CodeGeneratorContext ctx =
                    new CodeGeneratorContext(planner.getTableConfig())
                            .setOperatorBaseClass(TableStreamOperator.class);
            transformation =
                    ScanUtil.convertToInternalRow(
                            ctx,
                            (Transformation<Object>) sourceTransform,
                            fieldIndexes,
                            sourceType,
                            (RowType) getOutputType(),
                            qualifiedName,
                            JavaScalaConversionUtil.toScala(rowtimeExpr),
                            extractElement,
                            resetElement);
        } else {
            transformation = (Transformation<RowData>) sourceTransform;
        }
        return transformation;
    }

    private Optional<RexNode> getRowtimeExpression(FlinkRelBuilder relBuilder) {
        final List<Integer> fields =
                Arrays.stream(fieldIndexes).boxed().collect(Collectors.toList());
        if (!fields.contains(ROWTIME_STREAM_MARKER)) {
            return Optional.empty();
        } else {
            String rowtimeField = fieldNames[fields.indexOf(ROWTIME_STREAM_MARKER)];
            // get expression to extract timestamp
            LogicalType logicalType = fromDataTypeToLogicalType(sourceType);
            if (logicalType instanceof RowType) {
                RowType rowType = (RowType) logicalType;
                if (rowType.getFieldNames().contains(rowtimeField)
                        && TypeCheckUtils.isRowTime(
                                rowType.getTypeAt(rowType.getFieldIndex(rowtimeField)))) {
                    // if rowtimeField already existed in the data stream, use the default rowtime
                    return Optional.empty();
                }
            }
            return Optional.of(
                    relBuilder.cast(
                            relBuilder.call(new StreamRecordTimestampSqlFunction()),
                            relBuilder
                                    .getTypeFactory()
                                    .createFieldTypeFromLogicalType(
                                            new TimestampType(true, TimestampKind.ROWTIME, 3))
                                    .getSqlTypeName()));
        }
    }

    public DataStream<?> getDataStream() {
        return dataStream;
    }
}
