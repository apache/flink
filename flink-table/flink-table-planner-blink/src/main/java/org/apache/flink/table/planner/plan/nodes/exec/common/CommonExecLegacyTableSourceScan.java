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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.utils.ScanUtil;
import org.apache.flink.table.sources.DefinedFieldMapping;
import org.apache.flink.table.sources.InputFormatTableSource;
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.utils.TypeMappingUtils;

import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo;

/**
 * Base exec node to read data from an external source defined by a {@link StreamTableSource}.
 */
public interface CommonExecLegacyTableSourceScan {

	@SuppressWarnings("unchecked")
	default Transformation<?> getSourceTransformation(
			StreamExecutionEnvironment env,
			TableSource<?> tableSource) {
		if (tableSource instanceof InputFormatTableSource) {
			InputFormatTableSource<Object> inputFormat = (InputFormatTableSource<Object>) tableSource;
			TypeInformation<Object> typeInfo =
					(TypeInformation<Object>) fromDataTypeToTypeInfo(inputFormat.getProducedDataType());
			InputFormat<Object, ?> format = inputFormat.getInputFormat();
			return createInput(env, format, typeInfo);
		} else if (tableSource instanceof StreamTableSource) {
			return ((StreamTableSource<?>) tableSource).getDataStream(env).getTransformation();
		} else {
			throw new UnsupportedOperationException(tableSource.getClass().getSimpleName() + " is unsupported.");
		}
	}

	<IN> Transformation<IN> createInput(
			StreamExecutionEnvironment env,
			InputFormat<IN, ? extends InputSplit> inputFormat,
			TypeInformation<IN> typeInfo);

	default boolean needInternalConversion(TableSource<?> tableSource, int[] fieldIndexes) {
		return ScanUtil.hasTimeAttributeField(fieldIndexes) ||
				ScanUtil.needsConversion(tableSource.getProducedDataType());
	}

	default int[] computeIndexMapping(TableSource<?> tableSource, RowType outputType, boolean isStreaming) {
		TableSchema tableSchema = FlinkTypeFactory.toTableSchema(
				FlinkTypeFactory.INSTANCE().buildRelNodeRowType(outputType));
		return TypeMappingUtils.computePhysicalIndicesOrTimeAttributeMarkers(
				tableSource,
				tableSchema.getTableColumns(),
				isStreaming,
				getNameRemapping(tableSource)
		);
	}

	default Function<String, String> getNameRemapping(TableSource<?> tableSource) {
		if (tableSource instanceof DefinedFieldMapping) {
			Map<String, String> mapping = ((DefinedFieldMapping) tableSource).getFieldMapping();
			if (mapping != null) {
				return mapping::get;
			}
		}
		return Function.identity();
	}

}
