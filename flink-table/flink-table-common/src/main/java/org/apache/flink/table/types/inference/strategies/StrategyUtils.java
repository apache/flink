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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.table.types.logical.LogicalTypeFamily.APPROXIMATE_NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.EXACT_NUMERIC;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BINARY;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.CHAR;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE;
import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsImplicitCast;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getLength;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;

/**
 * Utilities for shared logic in classes of this package.
 */
final class StrategyUtils {

	/**
	 * Finds a data type that is close to the given data type in terms of nullability and conversion
	 * class but of the given logical root.
	 */
	static Optional<DataType> findDataType(
			CallContext callContext,
			boolean throwOnFailure,
			DataType actualDataType,
			LogicalTypeRoot expectedRoot,
			@Nullable Boolean expectedNullability) {
		final LogicalType actualType = actualDataType.getLogicalType();
		return Optional.ofNullable(findDataTypeOfRoot(actualDataType, expectedRoot))
			// set nullability
			.map(newDataType -> {
				if (Objects.equals(expectedNullability, Boolean.TRUE)) {
					return newDataType.nullable();
				} else if (Objects.equals(expectedNullability, Boolean.FALSE)) {
					return newDataType.notNull();
				} else if (actualType.isNullable()) {
					return newDataType.nullable();
				}
				return newDataType.notNull();
			})
			// preserve bridging class if possible
			.map(newDataType -> {
				final Class<?> clazz = actualDataType.getConversionClass();
				final LogicalType newType = newDataType.getLogicalType();
				if (newType.supportsOutputConversion(clazz)) {
					return newDataType.bridgedTo(clazz);
				}
				return newDataType;
			})
			// check if type can be implicitly casted
			.filter(newDataType -> {
				if (supportsImplicitCast(actualType, newDataType.getLogicalType())) {
					return true;
				}
				if (throwOnFailure) {
					throw callContext.newValidationError(
						"Unsupported argument type. Expected type root '%s' but actual type was '%s'.",
						expectedRoot,
						actualType);
				}
				return false;
			});
	}

	/**
	 * Returns a data type for the given data type and expected root.
	 *
	 * <p>This method is aligned with {@link LogicalTypeCasts#supportsImplicitCast(LogicalType, LogicalType)}.
	 *
	 * <p>The "fallback" data type for each root represents the default data type for a NULL literal. NULL
	 * literals will receive the smallest precision possible for having little impact when finding a common
	 * type. The output of this method needs to be checked again if an implicit cast is supported.
	 */
	private static @Nullable DataType findDataTypeOfRoot(
			DataType actualDataType,
			LogicalTypeRoot expectedRoot) {
		final LogicalType actualType = actualDataType.getLogicalType();
		if (hasRoot(actualType, expectedRoot)) {
			return actualDataType;
		}
		switch (expectedRoot) {
			case CHAR:
				return DataTypes.CHAR(CharType.DEFAULT_LENGTH);
			case VARCHAR:
				if (hasRoot(actualType, CHAR)) {
					return DataTypes.VARCHAR(getLength(actualType));
				}
				return DataTypes.VARCHAR(VarCharType.DEFAULT_LENGTH);
			case BOOLEAN:
				return DataTypes.BOOLEAN();
			case BINARY:
				return DataTypes.BINARY(BinaryType.DEFAULT_LENGTH);
			case VARBINARY:
				if (hasRoot(actualType, BINARY)) {
					return DataTypes.VARBINARY(getLength(actualType));
				}
				return DataTypes.VARBINARY(VarBinaryType.DEFAULT_LENGTH);
			case DECIMAL:
				if (hasFamily(actualType, EXACT_NUMERIC)) {
					return DataTypes.DECIMAL(getPrecision(actualType), getScale(actualType));
				} else if (hasFamily(actualType, APPROXIMATE_NUMERIC)) {
					final int precision = getPrecision(actualType);
					// we don't know where the precision occurs (before or after the dot)
					return DataTypes.DECIMAL(precision * 2, precision);
				}
				return DataTypes.DECIMAL(DecimalType.MIN_PRECISION, DecimalType.MIN_SCALE);
			case TINYINT:
				return DataTypes.TINYINT();
			case SMALLINT:
				return DataTypes.SMALLINT();
			case INTEGER:
				return DataTypes.INT();
			case BIGINT:
				return DataTypes.BIGINT();
			case FLOAT:
				return DataTypes.FLOAT();
			case DOUBLE:
				return DataTypes.DOUBLE();
			case DATE:
				return DataTypes.DATE();
			case TIME_WITHOUT_TIME_ZONE:
				if (hasRoot(actualType, TIMESTAMP_WITHOUT_TIME_ZONE)) {
					return DataTypes.TIME(getPrecision(actualType));
				}
				return DataTypes.TIME();
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return DataTypes.TIMESTAMP();
			case TIMESTAMP_WITH_TIME_ZONE:
				return DataTypes.TIMESTAMP_WITH_TIME_ZONE();
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE();
			case INTERVAL_YEAR_MONTH:
				return DataTypes.INTERVAL(DataTypes.MONTH());
			case INTERVAL_DAY_TIME:
				return DataTypes.INTERVAL(DataTypes.SECOND());
			case NULL:
				return DataTypes.NULL();
			case ARRAY:
			case MULTISET:
			case MAP:
			case ROW:
			case DISTINCT_TYPE:
			case STRUCTURED_TYPE:
			case RAW:
			case SYMBOL:
			case UNRESOLVED:
			default:
				return null;
		}
	}

	private StrategyUtils() {
		// no instantiation
	}
}
