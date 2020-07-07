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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType;

/**
 * built-in FirstValue aggregate function.
 */
public abstract class FirstValueAggFunction<T> extends AggregateFunction<T, GenericRowData> {

	@Override
	public boolean isDeterministic() {
		return false;
	}

	@Override
	public GenericRowData createAccumulator() {
		// The accumulator schema:
		// firstValue: T
		// firstOrder: Long
		GenericRowData acc = new GenericRowData(2);
		acc.setField(0, null);
		acc.setField(1, Long.MAX_VALUE);
		return acc;
	}

	public void accumulate(GenericRowData acc, Object value) {
		if (value != null && acc.getLong(1) == Long.MAX_VALUE) {
			acc.setField(0, value);
			acc.setField(1, System.currentTimeMillis());
		}
	}

	public void accumulate(GenericRowData acc, Object value, Long order) {
		if (value != null && acc.getLong(1) > order) {
			acc.setField(0, value);
			acc.setField(1, order);
		}
	}

	public void resetAccumulator(GenericRowData acc) {
		acc.setField(0, null);
		acc.setField(1, Long.MAX_VALUE);
	}

	@Override
	public T getValue(GenericRowData acc) {
		return (T) acc.getField(0);
	}

	@Override
	public TypeInformation<GenericRowData> getAccumulatorType() {
		LogicalType[] fieldTypes = new LogicalType[] {
				fromTypeInfoToLogicalType(getResultType()),
				new BigIntType()
		};

		String[] fieldNames = new String[] {
				"value",
				"time"
		};

		return (TypeInformation) new RowDataTypeInfo(fieldTypes, fieldNames);
	}

	/**
	 * Built-in Byte FirstValue aggregate function.
	 */
	public static class ByteFirstValueAggFunction extends FirstValueAggFunction<Byte> {

		@Override
		public TypeInformation<Byte> getResultType() {
			return Types.BYTE;
		}
	}

	/**
	 * Built-in Short FirstValue aggregate function.
	 */
	public static class ShortFirstValueAggFunction extends FirstValueAggFunction<Short> {

		@Override
		public TypeInformation<Short> getResultType() {
			return Types.SHORT;
		}
	}

	/**
	 * Built-in Int FirstValue aggregate function.
	 */
	public static class IntFirstValueAggFunction extends FirstValueAggFunction<Integer> {

		@Override
		public TypeInformation<Integer> getResultType() {
			return Types.INT;
		}
	}

	/**
	 * Built-in Long FirstValue aggregate function.
	 */
	public static class LongFirstValueAggFunction extends FirstValueAggFunction<Long> {

		@Override
		public TypeInformation<Long> getResultType() {
			return Types.LONG;
		}
	}

	/**
	 * Built-in Float FirstValue aggregate function.
	 */
	public static class FloatFirstValueAggFunction extends FirstValueAggFunction<Float> {

		@Override
		public TypeInformation<Float> getResultType() {
			return Types.FLOAT;
		}
	}

	/**
	 * Built-in Double FirstValue aggregate function.
	 */
	public static class DoubleFirstValueAggFunction extends FirstValueAggFunction<Double> {

		@Override
		public TypeInformation<Double> getResultType() {
			return Types.DOUBLE;
		}
	}

	/**
	 * Built-in Boolean FirstValue aggregate function.
	 */
	public static class BooleanFirstValueAggFunction extends FirstValueAggFunction<Boolean> {

		@Override
		public TypeInformation<Boolean> getResultType() {
			return Types.BOOLEAN;
		}
	}

	/**
	 * Built-in DecimalData FirstValue aggregate function.
	 */
	public static class DecimalFirstValueAggFunction extends FirstValueAggFunction<DecimalData> {

		private DecimalDataTypeInfo decimalTypeInfo;

		public DecimalFirstValueAggFunction(DecimalDataTypeInfo decimalTypeInfo) {
			this.decimalTypeInfo = decimalTypeInfo;
		}

		public void accumulate(GenericRowData acc, DecimalData value) {
			super.accumulate(acc, value);
		}

		public void accumulate(GenericRowData acc, DecimalData value, Long order) {
			super.accumulate(acc, value, order);
		}

		@Override
		public TypeInformation<DecimalData> getResultType() {
			return decimalTypeInfo;
		}
	}

	/**
	 * Built-in String FirstValue aggregate function.
	 */
	public static class StringFirstValueAggFunction extends FirstValueAggFunction<StringData> {

		@Override
		public TypeInformation<StringData> getResultType() {
			return StringDataTypeInfo.INSTANCE;
		}

		public void accumulate(GenericRowData acc, StringData value) {
			if (value != null) {
				super.accumulate(acc, ((BinaryStringData) value).copy());
			}
		}

		public void accumulate(GenericRowData acc, StringData value, Long order) {
			// just ignore nulls values and orders
			if (value != null) {
				super.accumulate(acc, ((BinaryStringData) value).copy(), order);
			}
		}
	}
}
