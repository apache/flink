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
 * built-in FirstValue aggregate function.
 */
public abstract class FirstValueAggFunction<T> extends AggregateFunction<T, GenericRow> {

	private static final long serialVersionUID = -6799132930154055151L;

	@Override
	public boolean isDeterministic() {
		return false;
	}

	@Override
	public GenericRow createAccumulator() {
		// The accumulator schema:
		// firstValue: T
		// firstOrder: Long
		GenericRow acc = new GenericRow(2);
		acc.setField(0, null);
		acc.setLong(1, Long.MAX_VALUE);
		return acc;
	}

	public void accumulate(GenericRow acc, Object value) {
		if (value != null && acc.getLong(1) == Long.MAX_VALUE) {
			acc.setField(0, value);
			acc.setLong(1, System.currentTimeMillis());
		}
	}

	public void accumulate(GenericRow acc, Object value, Long order) {
		if (value != null && acc.getLong(1) > order) {
			acc.setField(0, value);
			acc.setLong(1, order);
		}
	}

	public void resetAccumulator(GenericRow acc) {
		acc.setField(0, null);
		acc.setLong(1, Long.MAX_VALUE);
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
	 * Built-in Byte FirstValue aggregate function.
	 */
	public static class ByteFirstValueAggFunction extends FirstValueAggFunction<Byte> {

		private static final long serialVersionUID = 3936682589093183446L;

		@Override
		public TypeInformation<Byte> getResultType() {
			return Types.BYTE;
		}
	}

	/**
	 * Built-in Short FirstValue aggregate function.
	 */
	public static class ShortFirstValueAggFunction extends FirstValueAggFunction<Short> {

		private static final long serialVersionUID = -154324226234651483L;

		@Override
		public TypeInformation<Short> getResultType() {
			return Types.SHORT;
		}
	}

	/**
	 * Built-in Int FirstValue aggregate function.
	 */
	public static class IntFirstValueAggFunction extends FirstValueAggFunction<Integer> {

		private static final long serialVersionUID = 1535241164814113374L;

		@Override
		public TypeInformation<Integer> getResultType() {
			return Types.INT;
		}
	}

	/**
	 * Built-in Long FirstValue aggregate function.
	 */
	public static class LongFirstValueAggFunction extends FirstValueAggFunction<Long> {

		private static final long serialVersionUID = 763352549277863209L;

		@Override
		public TypeInformation<Long> getResultType() {
			return Types.LONG;
		}
	}

	/**
	 * Built-in Float FirstValue aggregate function.
	 */
	public static class FloatFirstValueAggFunction extends FirstValueAggFunction<Float> {

		private static final long serialVersionUID = 434009975151949222L;

		@Override
		public TypeInformation<Float> getResultType() {
			return Types.FLOAT;
		}
	}

	/**
	 * Built-in Double FirstValue aggregate function.
	 */
	public static class DoubleFirstValueAggFunction extends FirstValueAggFunction<Double> {

		private static final long serialVersionUID = -1499319409575049045L;

		@Override
		public TypeInformation<Double> getResultType() {
			return Types.DOUBLE;
		}
	}

	/**
	 * Built-in Boolean FirstValue aggregate function.
	 */
	public static class BooleanFirstValueAggFunction extends FirstValueAggFunction<Boolean> {

		private static final long serialVersionUID = 4256890950594337079L;

		@Override
		public TypeInformation<Boolean> getResultType() {
			return Types.BOOLEAN;
		}
	}

	/**
	 * Built-in Decimal FirstValue aggregate function.
	 */
	public static class DecimalFirstValueAggFunction extends FirstValueAggFunction<Decimal> {

		private static final long serialVersionUID = -1785743408763637461L;
		private DecimalTypeInfo decimalTypeInfo;

		public DecimalFirstValueAggFunction(DecimalTypeInfo decimalTypeInfo) {
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
	 * Built-in String FirstValue aggregate function.
	 */
	public static class StringFirstValueAggFunction extends FirstValueAggFunction<BinaryString> {

		private static final long serialVersionUID = -7711911518711017626L;

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
