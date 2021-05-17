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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.MultipleTransformationTranslator;
import org.apache.flink.table.planner.plan.utils.ScanUtil;
import org.apache.flink.table.planner.sources.TableSourceUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TypeMappingUtils;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo;

/**
 * Base {@link ExecNode} to read data from an external source defined by a {@link
 * StreamTableSource}.
 */
public abstract class CommonExecLegacyTableSourceScan extends ExecNodeBase<RowData>
        implements MultipleTransformationTranslator<RowData> {

    protected final TableSource<?> tableSource;
    protected final List<String> qualifiedName;

    public CommonExecLegacyTableSourceScan(
            TableSource<?> tableSource,
            List<String> qualifiedName,
            RowType outputType,
            String description) {
        super(Collections.emptyList(), outputType, description);
        this.tableSource = tableSource;
        this.qualifiedName = qualifiedName;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Transformation<RowData> translateToPlanInternal(PlannerBase planner) {
        final Transformation<?> sourceTransform;
        final StreamExecutionEnvironment env = planner.getExecEnv();
        if (tableSource instanceof InputFormatTableSource) {
            InputFormatTableSource<Object> inputFormat =
                    (InputFormatTableSource<Object>) tableSource;
            TypeInformation<Object> typeInfo =
                    (TypeInformation<Object>)
                            fromDataTypeToTypeInfo(inputFormat.getProducedDataType());
            InputFormat<Object, ?> format = inputFormat.getInputFormat();
            sourceTransform = createInput(env, format, typeInfo);
        } else if (tableSource instanceof StreamTableSource) {
            sourceTransform =
                    ((StreamTableSource<?>) tableSource).getDataStream(env).getTransformation();
        } else {
            throw new UnsupportedOperationException(
                    tableSource.getClass().getSimpleName() + " is unsupported.");
        }

        final TypeInformation<?> inputType = sourceTransform.getOutputType();
        final DataType producedDataType = tableSource.getProducedDataType();
        // check that declared and actual type of table source DataStream are identical
        if (!inputType.equals(TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(producedDataType))) {
            throw new TableException(
                    String.format(
                            "TableSource of type %s "
                                    + "returned a DataStream of data type %s that does not match with the "
                                    + "data type %s declared by the TableSource.getProducedDataType() method. "
                                    + "Please validate the implementation of the TableSource.",
                            tableSource.getClass().getCanonicalName(),
                            inputType,
                            producedDataType));
        }

        final RowType outputType = (RowType) getOutputType();
        final RelDataType relDataType = FlinkTypeFactory.INSTANCE().buildRelNodeRowType(outputType);
        // get expression to extract rowtime attribute
        final Optional<RexNode> rowtimeExpression =
                JavaScalaConversionUtil.toJava(
                                TableSourceUtil.getRowtimeAttributeDescriptor(
                                        tableSource, relDataType))
                        .map(
                                desc ->
                                        TableSourceUtil.getRowtimeExtractionExpression(
                                                desc.getTimestampExtractor(),
                                                producedDataType,
                                                planner.getRelBuilder(),
                                                getNameRemapping()));

        return createConversionTransformationIfNeeded(
                planner, sourceTransform, rowtimeExpression.orElse(null));
    }

    protected abstract <IN> Transformation<IN> createInput(
            StreamExecutionEnvironment env,
            InputFormat<IN, ? extends InputSplit> inputFormat,
            TypeInformation<IN> typeInfo);

    protected abstract Transformation<RowData> createConversionTransformationIfNeeded(
            PlannerBase planner,
            Transformation<?> sourceTransform,
            @Nullable RexNode rowtimeExpression);

    protected boolean needInternalConversion(int[] fieldIndexes) {
        return ScanUtil.hasTimeAttributeField(fieldIndexes)
                || ScanUtil.needsConversion(tableSource.getProducedDataType());
    }

    protected int[] computeIndexMapping(boolean isStreaming) {
        TableSchema tableSchema =
                FlinkTypeFactory.toTableSchema(
                        FlinkTypeFactory.INSTANCE().buildRelNodeRowType((RowType) getOutputType()));
        return TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
                tableSource, tableSchema.getTableColumns(), isStreaming, getNameRemapping());
    }

    private Function<String, String> getNameRemapping() {
        if (tableSource instanceof DefinedFieldMapping) {
            Map<String, String> mapping = ((DefinedFieldMapping) tableSource).getFieldMapping();
            if (mapping != null) {
                return mapping::get;
            }
        }
        return Function.identity();
    }
}
