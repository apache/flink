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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

/**
 * Implementation of {@link LogicalTypeVisitor} that redirects all calls to
 * {@link LogicalTypeDefaultVisitor#defaultMethod(LogicalType)}.
 */
@Internal
public abstract class LogicalTypeDefaultVisitor<R> implements LogicalTypeVisitor<R> {

	@Override
	public R visit(CharType charType) {
		return defaultMethod(charType);
	}

	@Override
	public R visit(VarCharType varCharType) {
		return defaultMethod(varCharType);
	}

	@Override
	public R visit(BooleanType booleanType) {
		return defaultMethod(booleanType);
	}

	@Override
	public R visit(BinaryType binaryType) {
		return defaultMethod(binaryType);
	}

	@Override
	public R visit(VarBinaryType varBinaryType) {
		return defaultMethod(varBinaryType);
	}

	@Override
	public R visit(DecimalType decimalType) {
		return defaultMethod(decimalType);
	}

	@Override
	public R visit(TinyIntType tinyIntType) {
		return defaultMethod(tinyIntType);
	}

	@Override
	public R visit(SmallIntType smallIntType) {
		return defaultMethod(smallIntType);
	}

	@Override
	public R visit(IntType intType) {
		return defaultMethod(intType);
	}

	@Override
	public R visit(BigIntType bigIntType) {
		return defaultMethod(bigIntType);
	}

	@Override
	public R visit(FloatType floatType) {
		return defaultMethod(floatType);
	}

	@Override
	public R visit(DoubleType doubleType) {
		return defaultMethod(doubleType);
	}

	@Override
	public R visit(DateType dateType) {
		return defaultMethod(dateType);
	}

	@Override
	public R visit(TimeType timeType) {
		return defaultMethod(timeType);
	}

	@Override
	public R visit(TimestampType timestampType) {
		return defaultMethod(timestampType);
	}

	@Override
	public R visit(ZonedTimestampType zonedTimestampType) {
		return defaultMethod(zonedTimestampType);
	}

	@Override
	public R visit(LocalZonedTimestampType localZonedTimestampType) {
		return defaultMethod(localZonedTimestampType);
	}

	@Override
	public R visit(YearMonthIntervalType yearMonthIntervalType) {
		return defaultMethod(yearMonthIntervalType);
	}

	@Override
	public R visit(DayTimeIntervalType dayTimeIntervalType) {
		return defaultMethod(dayTimeIntervalType);
	}

	@Override
	public R visit(ArrayType arrayType) {
		return defaultMethod(arrayType);
	}

	@Override
	public R visit(MultisetType multisetType) {
		return defaultMethod(multisetType);
	}

	@Override
	public R visit(MapType mapType) {
		return defaultMethod(mapType);
	}

	@Override
	public R visit(RowType rowType) {
		return defaultMethod(rowType);
	}

	@Override
	public R visit(DistinctType distinctType) {
		return defaultMethod(distinctType);
	}

	@Override
	public R visit(StructuredType structuredType) {
		return defaultMethod(structuredType);
	}

	@Override
	public R visit(NullType nullType) {
		return defaultMethod(nullType);
	}

	@Override
	public R visit(RawType<?> rawType) {
		return defaultMethod(rawType);
	}

	@Override
	public R visit(SymbolType<?> symbolType) {
		return defaultMethod(symbolType);
	}

	@Override
	public R visit(LogicalType other) {
		return defaultMethod(other);
	}

	protected abstract R defaultMethod(LogicalType logicalType);
}
