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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.planner.plan.utils.ScanUtil;
import org.apache.flink.table.planner.sources.TableSourceUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * Batch {@link ExecNode} to read data from an external source defined by a bounded {@link
 * StreamTableSource}.
 */
public class BatchExecLegacyTableSourceScan extends CommonExecLegacyTableSourceScan
        implements BatchExecNode<RowData> {

    public BatchExecLegacyTableSourceScan(
            TableSource<?> tableSource,
            List<String> qualifiedName,
            RowType outputType,
            String description) {
        super(tableSource, qualifiedName, outputType, description);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final Transformation<RowData> transformation = super.translateToPlanInternal(planner);
        // the boundedness has been checked via the stream table source already, so we can safely
        // declare all legacy transformations as bounded to make the stream graph generator happy
        ExecNodeUtil.makeLegacySourceTransformationsBounded(transformation);
        return transformation;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> createConversionTransformationIfNeeded(
            PlannerBase planner,
            Transformation<?> sourceTransform,
            @Nullable RexNode rowtimeExpression) {
        final int[] fieldIndexes = computeIndexMapping(false);
        if (needInternalConversion(fieldIndexes)) {
            // the produced type may not carry the correct precision user defined in DDL, because
            // it may be converted from legacy type. Fix precision using logical schema from DDL.
            // code generation requires the correct precision of input fields.
            DataType fixedProducedDataType =
                    TableSourceUtil.fixPrecisionForProducedDataType(
                            tableSource, (RowType) getOutputType());
            return ScanUtil.convertToInternalRow(
                    new CodeGeneratorContext(planner.getTableConfig()),
                    (Transformation<Object>) sourceTransform,
                    fieldIndexes,
                    fixedProducedDataType,
                    (RowType) getOutputType(),
                    qualifiedName,
                    JavaScalaConversionUtil.toScala(Optional.ofNullable(rowtimeExpression)),
                    "",
                    "");

        } else {
            return (Transformation<RowData>) sourceTransform;
        }
    }

    @Override
    protected <IN> Transformation<IN> createInput(
            StreamExecutionEnvironment env,
            InputFormat<IN, ? extends InputSplit> inputFormat,
            TypeInformation<IN> typeInfo) {
        // env.createInput will use ContinuousFileReaderOperator, but it do not support multiple
        // paths. If read partitioned source, after partition pruning, we need let InputFormat
        // to read multiple partitions which are multiple paths.
        // We can use InputFormatSourceFunction directly to support InputFormat.
        final InputFormatSourceFunction<IN> function =
                new InputFormatSourceFunction<>(inputFormat, typeInfo);
        return env.addSource(function, tableSource.explainSource(), typeInfo).getTransformation();
    }
}
