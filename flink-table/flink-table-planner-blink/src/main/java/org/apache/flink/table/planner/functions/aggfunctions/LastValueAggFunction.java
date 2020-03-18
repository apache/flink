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
import org.apache.flink.table.dataformat.BinaryString;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.runtime.typeutils.BinaryStringTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType;

/**
 * built-in LastValue aggregate function.
 */
public class LastValueAggFunction<T> extends AggregateFunction<T, GenericRow> {

	@Override
	public boolean isDeterministic() {
		return false;
	}

	@Override
	public GenericRow createAccumulator() {
		// The accumulator schema:
		// lastValue: T
		// lastOrder: Long
		GenericRow acc = new GenericRow(2);
		acc.setField(0, null);
		acc.setLong(1, Long.MIN_VALUE);
		return acc;
	}

	public void accumulate(GenericRow acc, Object value) {
		if (value != null) {
			acc.setField(0, value);
		}
	}

	public void accumulate(GenericRow acc, Object value, Long order) {
		if (value != null && acc.getLong(1) < order) {
			acc.setField(0, value);
			acc.setLong(1, order);
		}
	}

	public void resetAccumulator(GenericRow acc) {
		acc.setField(0, null);
		acc.setLong(1, Long.MIN_VALUE);
	}

	@Override
	public T getValue(GenericRow acc) {
		return (T) acc.getField(0);
	}

	@Override
	public TypeInformation<GenericRow> getAccumulatorType() {
		LogicalType[] fieldTypes = new LogicalType[] {
				fromTypeInfoToLogicalType(getResultType()),
				new BigIntType()
		};

		String[] fieldNames = new String[] {
				"value",
				"time"
		};

		return (TypeInformation) new BaseRowTypeInfo(fieldTypes, fieldNames);
	}

	/**
	 * Built-in Byte LastValue aggregate function.
	 */
	public static class ByteLastValueAggFunction extends LastValueAggFunction<Byte> {

		@Override
		public TypeInformation<Byte> getResultType() {
			return Types.BYTE;
		}
	}

	/**
	 * Built-in Short LastValue aggregate function.
	 */
	public static class ShortLastValueAggFunction extends LastValueAggFunction<Short> {

		@Override
		public TypeInformation<Short> getResultType() {
			return Types.SHORT;
		}
	}

	/**
	 * Built-in Int LastValue aggregate function.
	 */
	public static class IntLastValueAggFunction extends LastValueAggFunction<Integer> {

		@Override
		public TypeInformation<Integer> getResultType() {
			return Types.INT;
		}
	}

	/**
	 * Built-in Long LastValue aggregate function.
	 */
	public static class LongLastValueAggFunction extends LastValueAggFunction<Long> {

		@Override
		public TypeInformation<Long> getResultType() {
			return Types.LONG;
		}
	}

	/**
	 * Built-in Float LastValue aggregate function.
	 */
	public static class FloatLastValueAggFunction extends LastValueAggFunction<Float> {

		@Override
		public TypeInformation<Float> getResultType() {
			return Types.FLOAT;
		}
	}

	/**
	 * Built-in Double LastValue aggregate function.
	 */
	public static class DoubleLastValueAggFunction extends LastValueAggFunction<Double> {

		@Override
		public TypeInformation<Double> getResultType() {
			return Types.DOUBLE;
		}
	}

	/**
	 * Built-in Boolean LastValue aggregate function.
	 */
	public static class BooleanLastValueAggFunction extends LastValueAggFunction<Boolean> {

		@Override
		public TypeInformation<Boolean> getResultType() {
			return Types.BOOLEAN;
		}
	}

	/**
	 * Built-in Decimal LastValue aggregate function.
	 */
	public static class DecimalLastValueAggFunction extends LastValueAggFunction<Decimal> {

		private DecimalTypeInfo decimalTypeInfo;

		public DecimalLastValueAggFunction(DecimalTypeInfo decimalTypeInfo) {
			this.decimalTypeInfo = decimalTypeInfo;
		}

		public void accumulate(GenericRow acc, Decimal value) {
			super.accumulate(acc, value);
		}

		public void accumulate(GenericRow acc, Decimal value, Long order) {
			super.accumulate(acc, value, order);
		}

		@Override
		public TypeInformation<Decimal> getResultType() {
			return decimalTypeInfo;
		}
	}


	/**
	 * Built-in String LastValue aggregate function.
	 */
	public static class StringLastValueAggFunction extends LastValueAggFunction<BinaryString> {

		@Override
		public TypeInformation<BinaryString> getResultType() {
			return BinaryStringTypeInfo.INSTANCE;
		}

		public void accumulate(GenericRow acc, BinaryString value) {
			if (value != null) {
				super.accumulate(acc, value.copy());
			}
		}

		public void accumulate(GenericRow acc, BinaryString value, Long order) {
			// just ignore nulls values and orders
			if (value != null) {
				super.accumulate(acc, value.copy(), order);
			}
		}
	}
}
