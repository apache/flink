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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.utils.ScanUtil;
import org.apache.flink.table.planner.sources.TableSourceUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.TableStreamOperator;
import org.apache.flink.table.runtime.operators.wmassigners.PeriodicWatermarkAssignerWrapper;
import org.apache.flink.table.runtime.operators.wmassigners.PunctuatedWatermarkAssignerWrapper;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.wmstrategies.PeriodicWatermarkAssigner;
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Stream {@link ExecNode} to read data from an external source defined by a {@link
 * StreamTableSource}.
 */
public class StreamExecLegacyTableSourceScan extends CommonExecLegacyTableSourceScan
        implements StreamExecNode<RowData> {

    public StreamExecLegacyTableSourceScan(
            TableSource<?> tableSource,
            List<String> qualifiedName,
            RowType outputType,
            String description) {
        super(tableSource, qualifiedName, outputType, description);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> createConversionTransformationIfNeeded(
            PlannerBase planner,
            Transformation<?> sourceTransform,
            @Nullable RexNode rowtimeExpression) {

        final RowType outputType = (RowType) getOutputType();
        final Transformation<RowData> transformation;
        final int[] fieldIndexes = computeIndexMapping(true);
        if (needInternalConversion(fieldIndexes)) {
            final String extractElement, resetElement;
            if (ScanUtil.hasTimeAttributeField(fieldIndexes)) {
                String elementTerm = OperatorCodeGenerator.ELEMENT();
                extractElement = String.format("ctx.%s = %s;", elementTerm, elementTerm);
                resetElement = String.format("ctx.%s = null;", elementTerm);
            } else {
                extractElement = "";
                resetElement = "";
            }

            final CodeGeneratorContext ctx =
                    new CodeGeneratorContext(planner.getTableConfig())
                            .setOperatorBaseClass(TableStreamOperator.class);
            // the produced type may not carry the correct precision user defined in DDL, because
            // it may be converted from legacy type. Fix precision using logical schema from DDL.
            // Code generation requires the correct precision of input fields.
            final DataType fixedProducedDataType =
                    TableSourceUtil.fixPrecisionForProducedDataType(tableSource, outputType);
            final Configuration config = planner.getTableConfig().getConfiguration();
            transformation =
                    ScanUtil.convertToInternalRow(
                            ctx,
                            (Transformation<Object>) sourceTransform,
                            fieldIndexes,
                            fixedProducedDataType,
                            outputType,
                            qualifiedName,
                            (detailName, simplifyName) ->
                                    getFormattedOperatorName(detailName, simplifyName, config),
                            (description) -> getFormattedOperatorDescription(description, config),
                            JavaScalaConversionUtil.toScala(Optional.ofNullable(rowtimeExpression)),
                            extractElement,
                            resetElement);
        } else {
            transformation = (Transformation<RowData>) sourceTransform;
        }

        final RelDataType relDataType = FlinkTypeFactory.INSTANCE().buildRelNodeRowType(outputType);
        final DataStream<RowData> ingestedTable =
                new DataStream<>(planner.getExecEnv(), transformation);
        final Optional<RowtimeAttributeDescriptor> rowtimeDesc =
                JavaScalaConversionUtil.toJava(
                        TableSourceUtil.getRowtimeAttributeDescriptor(tableSource, relDataType));

        final DataStream<RowData> withWatermarks =
                rowtimeDesc
                        .map(
                                desc -> {
                                    int rowtimeFieldIdx =
                                            relDataType
                                                    .getFieldNames()
                                                    .indexOf(desc.getAttributeName());
                                    WatermarkStrategy strategy = desc.getWatermarkStrategy();
                                    if (strategy instanceof PeriodicWatermarkAssigner) {
                                        PeriodicWatermarkAssignerWrapper watermarkGenerator =
                                                new PeriodicWatermarkAssignerWrapper(
                                                        (PeriodicWatermarkAssigner) strategy,
                                                        rowtimeFieldIdx);
                                        return ingestedTable.assignTimestampsAndWatermarks(
                                                watermarkGenerator);
                                    } else if (strategy instanceof PunctuatedWatermarkAssigner) {
                                        PunctuatedWatermarkAssignerWrapper watermarkGenerator =
                                                new PunctuatedWatermarkAssignerWrapper(
                                                        (PunctuatedWatermarkAssigner) strategy,
                                                        rowtimeFieldIdx,
                                                        tableSource.getProducedDataType());
                                        return ingestedTable.assignTimestampsAndWatermarks(
                                                watermarkGenerator);
                                    } else {
                                        // The watermarks have already been provided by the
                                        // underlying DataStream.
                                        return ingestedTable;
                                    }
                                })
                        .orElse(ingestedTable); // No need to generate watermarks if no rowtime
        // attribute is specified.
        return withWatermarks.getTransformation();
    }

    @Override
    protected <IN> Transformation<IN> createInput(
            StreamExecutionEnvironment env,
            InputFormat<IN, ? extends InputSplit> format,
            TypeInformation<IN> typeInfo) {
        // See StreamExecutionEnvironment.createInput, it is better to deal with checkpoint.
        // The disadvantage is that streaming not support multi-paths.
        return env.createInput(format, typeInfo)
                .name(tableSource.explainSource())
                .getTransformation();
    }
}
