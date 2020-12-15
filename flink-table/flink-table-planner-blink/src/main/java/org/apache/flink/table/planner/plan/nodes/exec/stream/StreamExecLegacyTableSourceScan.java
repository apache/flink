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
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.OperatorCodeGenerator;
import org.apache.flink.table.planner.delegation.StreamPlanner;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecLegacyTableSourceScan;
import org.apache.flink.table.planner.plan.utils.ScanUtil;
import org.apache.flink.table.planner.sources.TableSourceUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.operators.AbstractProcessStreamOperator;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.sources.RowtimeAttributeDescriptor;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.sources.wmstrategies.PeriodicWatermarkAssigner;
import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner;
import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Stream exec node to read data from an external source defined by a {@link StreamTableSource}.
 */
public class StreamExecLegacyTableSourceScan
		extends StreamExecNode<RowData>
		implements CommonExecLegacyTableSourceScan {
	private final TableSource<?> tableSource;
	private final List<String> qualifiedName;

	public StreamExecLegacyTableSourceScan(
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
	protected Transformation<RowData> translateToPlanInternal(StreamPlanner planner) {
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
			final String extractElement, resetElement;
			if (ScanUtil.hasTimeAttributeField(fieldIndexes)) {
				String elementTerm = OperatorCodeGenerator.ELEMENT();
				extractElement = String.format("ctx.%s = %s;", elementTerm, elementTerm);
				resetElement = String.format("ctx.%s = null;", elementTerm);
			} else {
				extractElement = "";
				resetElement = "";
			}

			CodeGeneratorContext ctx = new CodeGeneratorContext(planner.getTableConfig())
					.setOperatorBaseClass(AbstractProcessStreamOperator.class);
			// the produced type may not carry the correct precision user defined in DDL, because
			// it may be converted from legacy type. Fix precision using logical schema from DDL.
			// Code generation requires the correct precision of input fields.
			DataType fixedProducedDataType = TableSourceUtil.fixPrecisionForProducedDataType(
					tableSource, outputType);
			transformation = ScanUtil.convertToInternalRow(
					ctx,
					(Transformation<Object>) sourceTransform,
					fieldIndexes,
					fixedProducedDataType,
					outputType,
					qualifiedName,
					JavaScalaConversionUtil.toScala(rowtimeExpression),
					extractElement,
					resetElement);
		} else {
			transformation = (Transformation<RowData>) sourceTransform;
		}

		final DataStream<RowData> ingestedTable = new DataStream<>(planner.getExecEnv(), transformation);
		final Optional<RowtimeAttributeDescriptor> rowtimeDesc = JavaScalaConversionUtil.toJava(
				TableSourceUtil.getRowtimeAttributeDescriptor(tableSource, relDataType));

		final DataStream<RowData> withWatermarks = rowtimeDesc.map(desc -> {
			int rowtimeFieldIdx = relDataType.getFieldNames().indexOf(desc.getAttributeName());
			WatermarkStrategy strategy = desc.getWatermarkStrategy();
			if (strategy instanceof PeriodicWatermarkAssigner) {
				PeriodicWatermarkAssignerWrapper watermarkGenerator =
						new PeriodicWatermarkAssignerWrapper((PeriodicWatermarkAssigner) strategy, rowtimeFieldIdx);
				return ingestedTable.assignTimestampsAndWatermarks(watermarkGenerator);
			} else if (strategy instanceof PunctuatedWatermarkAssigner) {
				PunctuatedWatermarkAssignerWrapper watermarkGenerator =
						new PunctuatedWatermarkAssignerWrapper(
								(PunctuatedWatermarkAssigner) strategy, rowtimeFieldIdx, producedDataType);
				return ingestedTable.assignTimestampsAndWatermarks(watermarkGenerator);
			} else {
				// The watermarks have already been provided by the underlying DataStream.
				return ingestedTable;
			}
		}).orElse(ingestedTable); // No need to generate watermarks if no rowtime attribute is specified.
		return withWatermarks.getTransformation();
	}

	@Override
	public <IN> Transformation<IN> createInput(
			StreamExecutionEnvironment env,
			InputFormat<IN, ? extends InputSplit> format,
			TypeInformation<IN> typeInfo) {
		// See StreamExecutionEnvironment.createInput, it is better to deal with checkpoint.
		// The disadvantage is that streaming not support multi-paths.
		return env.createInput(format, typeInfo).name(tableSource.explainSource()).getTransformation();
	}

	/**
	 * Generates periodic watermarks based on a {@link PeriodicWatermarkAssigner}.
	 */
	private static class PeriodicWatermarkAssignerWrapper implements AssignerWithPeriodicWatermarks<RowData> {
		private static final long serialVersionUID = 1L;
		private final PeriodicWatermarkAssigner assigner;
		private final int timeFieldIdx;

		/**
		 * @param timeFieldIdx the index of the rowtime attribute.
		 * @param assigner the watermark assigner.
		 */
		private PeriodicWatermarkAssignerWrapper(PeriodicWatermarkAssigner assigner, int timeFieldIdx) {
			this.assigner = assigner;
			this.timeFieldIdx = timeFieldIdx;
		}

		@Nullable
		@Override
		public Watermark getCurrentWatermark() {
			return assigner.getWatermark();
		}

		@Override
		public long extractTimestamp(RowData row, long recordTimestamp) {
			long timestamp = row.getTimestamp(timeFieldIdx, 3).getMillisecond();
			assigner.nextTimestamp(timestamp);
			return 0;
		}
	}

	/**
	 * Generates periodic watermarks based on a [[PunctuatedWatermarkAssigner]].
	 */
	private static class PunctuatedWatermarkAssignerWrapper implements AssignerWithPunctuatedWatermarks<RowData> {
		private static final long serialVersionUID = 1L;
		private final PunctuatedWatermarkAssigner assigner;
		private final int timeFieldIdx;
		private final DataFormatConverters.DataFormatConverter<RowData, Row> converter;

		/**
		 * @param timeFieldIdx the index of the rowtime attribute.
		 * @param assigner the watermark assigner.
		 * @param sourceType the type of source
		 */
		@SuppressWarnings("unchecked")
		private PunctuatedWatermarkAssignerWrapper(
				PunctuatedWatermarkAssigner assigner,
				int timeFieldIdx,
				DataType sourceType) {
			this.assigner = assigner;
			this.timeFieldIdx = timeFieldIdx;
			DataType originDataType;
			if (sourceType instanceof FieldsDataType) {
				originDataType = sourceType;
			} else {
				originDataType = DataTypes.ROW(DataTypes.FIELD("f0", sourceType));
			}
			converter = DataFormatConverters.getConverterForDataType(originDataType.bridgedTo(Row.class));
		}

		@Nullable
		@Override
		public Watermark checkAndGetNextWatermark(RowData row, long extractedTimestamp) {
			long timestamp = row.getLong(timeFieldIdx);
			return assigner.getWatermark(converter.toExternal(row), timestamp);
		}

		@Override
		public long extractTimestamp(RowData element, long recordTimestamp) {
			return 0;
		}
	}
}
