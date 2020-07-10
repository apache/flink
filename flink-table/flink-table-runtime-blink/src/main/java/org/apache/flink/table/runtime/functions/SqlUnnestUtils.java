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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.DataTypeUtils;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * Utility for handling SQL's {@code UNNEST}.
 */
@Internal
public final class SqlUnnestUtils {

	public static UnnestTableFunction createUnnestFunction(LogicalType t) {
		switch (t.getTypeRoot()) {
			case ARRAY:
				final ArrayType arrayType = (ArrayType) t;
				return new CollectionUnnestTableFunction(
					arrayType,
					arrayType.getElementType(),
					ArrayData.createElementGetter(arrayType.getElementType()));
			case MULTISET:
				final MultisetType multisetType = (MultisetType) t;
				return new CollectionUnnestTableFunction(
					multisetType,
					multisetType.getElementType(),
					ArrayData.createElementGetter(multisetType.getElementType()));
			case MAP:
				final MapType mapType = (MapType) t;
				return new MapUnnestTableFunction(
					mapType,
					RowType.of(false, mapType.getKeyType(), mapType.getValueType()),
					ArrayData.createElementGetter(mapType.getKeyType()),
					ArrayData.createElementGetter(mapType.getKeyType()));
			default:
				throw new UnsupportedOperationException("Unsupported type for UNNEST: " + t);
		}
	}

	/**
	 * Base table function for all unnest functions.
	 */
	public abstract static class UnnestTableFunction extends TableFunction<Object> {

		private final transient LogicalType inputType;

		private final transient LogicalType outputType;

		UnnestTableFunction(LogicalType inputType, LogicalType outputType) {
			this.inputType = inputType;
			this.outputType = outputType;
		}

		public LogicalType getWrappedOutputType() {
			if (hasRoot(outputType, LogicalTypeRoot.ROW) ||
					hasRoot(outputType, LogicalTypeRoot.STRUCTURED_TYPE)) {
				return outputType;
			}
			return RowType.of(outputType);
		}

		@Override
		public TypeInference getTypeInference(DataTypeFactory typeFactory) {
			final DataType inputDataType = DataTypeUtils.toInternalDataType(inputType);
			final DataType outputDataType = DataTypeUtils.toInternalDataType(outputType);
			return TypeInference.newBuilder()
				.typedArguments(inputDataType)
				.outputTypeStrategy(TypeStrategies.explicit(outputDataType))
				.build();
		}
	}

	/**
	 * Table function that unwraps the elements of a collection (array or multiset).
	 */
	public static final class CollectionUnnestTableFunction extends UnnestTableFunction {

		private static final long serialVersionUID = 1L;

		private final ArrayData.ElementGetter elementGetter;

		public CollectionUnnestTableFunction(
				LogicalType inputType,
				LogicalType outputType,
				ArrayData.ElementGetter elementGetter) {
			super(inputType, outputType);
			this.elementGetter = elementGetter;
		}

		public void eval(ArrayData arrayData) {
			if (arrayData == null) {
				return;
			}
			final int size = arrayData.size();
			for (int pos = 0; pos < size; pos++) {
				collect(elementGetter.getElementOrNull(arrayData, pos));
			}
		}

		public void eval(MapData mapData) {
			if (mapData == null) {
				return;
			}
			final int size = mapData.size();
			final ArrayData keys = mapData.keyArray();
			final ArrayData values = mapData.valueArray();
			for (int pos = 0; pos < size; pos++) {
				final int multiplier = values.getInt(pos);
				final Object key = elementGetter.getElementOrNull(keys, pos);
				for (int i = 0; i < multiplier; i++) {
					collect(key);
				}
			}
		}
	}

	/**
	 * Table function that unwraps the elements of a map.
	 */
	public static final class MapUnnestTableFunction extends UnnestTableFunction {

		private static final long serialVersionUID = 1L;

		private final ArrayData.ElementGetter keyGetter;

		private final ArrayData.ElementGetter valueGetter;

		public MapUnnestTableFunction(
				LogicalType inputType,
				LogicalType outputType,
				ArrayData.ElementGetter keyGetter,
				ArrayData.ElementGetter valueGetter) {
			super(inputType, outputType);
			this.keyGetter = keyGetter;
			this.valueGetter = valueGetter;
		}

		public void eval(MapData mapData) {
			if (mapData == null) {
				return;
			}
			final int size = mapData.size();
			final ArrayData keyArray = mapData.keyArray();
			final ArrayData valueArray = mapData.valueArray();
			for (int i = 0; i < size; i++) {
				collect(
					GenericRowData.of(
						keyGetter.getElementOrNull(keyArray, i),
						valueGetter.getElementOrNull(valueArray, i)));
			}
		}
	}

	private SqlUnnestUtils() {
		// no instantiation
	}
}
