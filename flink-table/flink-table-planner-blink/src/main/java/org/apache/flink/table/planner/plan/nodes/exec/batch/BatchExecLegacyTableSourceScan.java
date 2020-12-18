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
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.delegation.BatchPlanner;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.utils.ScanUtil;
import org.apache.flink.table.planner.sources.TableSourceUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Batch exec node to read data from an external source defined by a bounded {@link StreamTableSource}.
 */
public class BatchExecLegacyTableSourceScan
		extends BatchExecNode<RowData>
		implements CommonExecLegacyTableSourceScan {
	private final TableSource<?> tableSource;
	private final List<String> qualifiedName;

	public BatchExecLegacyTableSourceScan(
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
	protected Transformation<RowData> translateToPlanInternal(BatchPlanner planner) {
		final Transformation<?> sourceTransform = getSourceTransformation(planner.getExecEnv(), tableSource);

		final TypeInformation<?> inputType = sourceTransform.getOutputType();
		final DataType producedDataType = tableSource.getProducedDataType();
		// check that declared and actual type of table source DataStream are identical
		if (!inputType.equals(TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(producedDataType))) {
			throw new TableException(String.format("TableSource of type %s " +
							"returned a DataStream of data type %s that does not match with the " +
							"data type %s declared by the TableSource.getProducedDataType() method. " +
							"Please validate the implementation of the TableSource.",
					tableSource.getClass().getCanonicalName(), inputType, producedDataType));
		}

		final RowType outputType = (RowType) getOutputType();
		final RelDataType relDataType = FlinkTypeFactory.INSTANCE().buildRelNodeRowType(outputType);
		// get expression to extract rowtime attribute
		final Optional<RexNode> rowtimeExpression = JavaScalaConversionUtil.toJava(
				TableSourceUtil.getRowtimeAttributeDescriptor(tableSource, relDataType))
				.map(desc -> TableSourceUtil.getRowtimeExtractionExpression(
						desc.getTimestampExtractor(),
						producedDataType,
						planner.getRelBuilder(),
						getNameRemapping(tableSource)
				));

		final Transformation<RowData> transformation;
		final int[] fieldIndexes = computeIndexMapping(tableSource, outputType, true);
		if (needInternalConversion(tableSource, fieldIndexes)) {
			// the produced type may not carry the correct precision user defined in DDL, because
			// it may be converted from legacy type. Fix precision using logical schema from DDL.
			// code generation requires the correct precision of input fields.
			DataType fixedProducedDataType = TableSourceUtil.fixPrecisionForProducedDataType(
					tableSource, outputType);
			transformation = ScanUtil.convertToInternalRow(
					new CodeGeneratorContext(planner.getTableConfig()),
					(Transformation<Object>) sourceTransform,
					fieldIndexes,
					fixedProducedDataType,
					outputType,
					qualifiedName,
					JavaScalaConversionUtil.toScala(rowtimeExpression),
					"",
					"");

		} else {
			transformation = (Transformation<RowData>) sourceTransform;
		}
		return transformation;
	}

	@Override
	public <IN> Transformation<IN> createInput(
			StreamExecutionEnvironment env,
			InputFormat<IN, ? extends InputSplit> inputFormat,
			TypeInformation<IN> typeInfo) {
		// env.createInput will use ContinuousFileReaderOperator, but it do not support multiple
		// paths. If read partitioned source, after partition pruning, we need let InputFormat
		// to read multiple partitions which are multiple paths.
		// We can use InputFormatSourceFunction directly to support InputFormat.
		InputFormatSourceFunction<IN> function = new InputFormatSourceFunction<>(inputFormat, typeInfo);
		return env.addSource(function, tableSource.explainSource(), typeInfo).getTransformation();
	}
}
