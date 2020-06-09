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

package org.apache.flink.table.types.inference;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.strategies.CommonTypeStrategy;
import org.apache.flink.table.types.inference.strategies.ExplicitTypeStrategy;
import org.apache.flink.table.types.inference.strategies.FirstTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MappingTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MatchFamilyTypeStrategy;
import org.apache.flink.table.types.inference.strategies.MissingTypeStrategy;
import org.apache.flink.table.types.inference.strategies.NullableTypeStrategy;
import org.apache.flink.table.types.inference.strategies.UseArgumentTypeStrategy;
import org.apache.flink.table.types.inference.strategies.VaryingStringTypeStrategy;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getLength;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasScale;
import static org.apache.flink.table.types.logical.utils.LogicalTypeMerging.findCommonType;
import static org.apache.flink.table.types.utils.TypeConversions.fromLogicalToDataType;

/**
 * Strategies for inferring an output or accumulator data type of a function call.
 *
 * @see TypeStrategy
 */
@Internal
public final class TypeStrategies {

	/**
	 * Placeholder for a missing type strategy.
	 */
	public static final TypeStrategy MISSING = new MissingTypeStrategy();

	public static final TypeStrategy COMMON = new CommonTypeStrategy();

	/**
	 * Type strategy that returns a fixed {@link DataType}.
	 */
	public static TypeStrategy explicit(DataType dataType) {
		return new ExplicitTypeStrategy(dataType);
	}

	/**
	 * Type strategy that returns the n-th input argument.
	 */
	public static TypeStrategy argument(int pos) {
		return new UseArgumentTypeStrategy(pos);
	}

	/**
	 * Type strategy that returns the first type that could be inferred.
	 */
	public static TypeStrategy first(TypeStrategy... strategies) {
		return new FirstTypeStrategy(Arrays.asList(strategies));
	}

	/**
	 * Type strategy that returns the given argument if it is of the same logical type family.
	 */
	public static TypeStrategy matchFamily(int argumentPos, LogicalTypeFamily family) {
		return new MatchFamilyTypeStrategy(argumentPos, family);
	}

	/**
	 * Type strategy that maps an {@link InputTypeStrategy} to a {@link TypeStrategy} if the input strategy
	 * infers identical types.
	 */
	public static TypeStrategy mapping(Map<InputTypeStrategy, TypeStrategy> mappings) {
		return new MappingTypeStrategy(mappings);
	}

	/**
	 * A type strategy that can be used to make a result type nullable if any of the selected
	 * input arguments is nullable. Otherwise the type will be not null.
	 */
	public static TypeStrategy nullable(ConstantArgumentCount includedArgs, TypeStrategy initialStrategy) {
		return new NullableTypeStrategy(includedArgs, initialStrategy);
	}

	/**
	 * A type strategy that can be used to make a result type nullable if any of the
	 * input arguments is nullable. Otherwise the type will be not null.
	 */
	public static TypeStrategy nullable(TypeStrategy initialStrategy) {
		return nullable(ConstantArgumentCount.any(), initialStrategy);
	}

	/**
	 * A type strategy that ensures that the result type is either {@link LogicalTypeRoot#VARCHAR} or
	 * {@link LogicalTypeRoot#VARBINARY} from their corresponding non-varying roots.
	 */
	public static TypeStrategy varyingString(TypeStrategy initialStrategy) {
		return new VaryingStringTypeStrategy(initialStrategy);
	}

	// --------------------------------------------------------------------------------------------
	// Specific type strategies
	// --------------------------------------------------------------------------------------------

	/**
	 * Type strategy that returns a {@link DataTypes#ROW()} with fields types equal to input types.
	 */
	public static final TypeStrategy ROW = callContext -> {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		DataTypes.Field[] fields = IntStream.range(0, argumentDataTypes.size())
			.mapToObj(idx -> DataTypes.FIELD("f" + idx, argumentDataTypes.get(idx)))
			.toArray(DataTypes.Field[]::new);

		return Optional.of(DataTypes.ROW(fields).notNull());
	};

	/**
	 * Type strategy that returns a {@link DataTypes#MAP(DataType, DataType)} with a key type equal to type
	 * of the first argument and a value type equal to the type of second argument.
	 */
	public static final TypeStrategy MAP = callContext -> {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		if (argumentDataTypes.size() < 2) {
			return Optional.empty();
		}
		return Optional.of(DataTypes.MAP(argumentDataTypes.get(0), argumentDataTypes.get(1)).notNull());
	};

	/**
	 * Type strategy that returns a {@link DataTypes#ARRAY(DataType)} with element type equal to the type of
	 * the first argument.
	 */
	public static final TypeStrategy ARRAY = callContext -> {
		List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		if (argumentDataTypes.size() < 1) {
			return Optional.empty();
		}
		return Optional.of(DataTypes.ARRAY(argumentDataTypes.get(0)).notNull());
	};

	/**
<<<<<<< HEAD
	 * Type strategy that returns the sum of an exact numeric addition that includes at least one decimal.
	 */
	public static final TypeStrategy DECIMAL_PLUS = callContext -> {
		final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		final LogicalType addend1 = argumentDataTypes.get(0).getLogicalType();
		final LogicalType addend2 = argumentDataTypes.get(1).getLogicalType();
		// a hack to make legacy types possible until we drop them
		if (addend1 instanceof LegacyTypeInformationType) {
			return Optional.of(argumentDataTypes.get(0));
		}
		if (addend2 instanceof LegacyTypeInformationType) {
			return Optional.of(argumentDataTypes.get(1));
		}
		if (!isDecimalComputation(addend1, addend2)) {
			return Optional.empty();
		}
		final DecimalType decimalType = LogicalTypeMerging.findAdditionDecimalType(
			getPrecision(addend1),
			getScale(addend1),
			getPrecision(addend2),
			getScale(addend2));
		return Optional.of(fromLogicalToDataType(decimalType));
	};

	/**
	 * Type strategy that returns the quotient of an exact numeric division that includes at least
	 * one decimal.
	 */
	public static final TypeStrategy DECIMAL_DIVIDE = callContext -> {
		final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		final LogicalType dividend = argumentDataTypes.get(0).getLogicalType();
		final LogicalType divisor = argumentDataTypes.get(1).getLogicalType();
		// a hack to make legacy types possible until we drop them
		if (dividend instanceof LegacyTypeInformationType) {
			return Optional.of(argumentDataTypes.get(0));
		}
		if (divisor instanceof LegacyTypeInformationType) {
			return Optional.of(argumentDataTypes.get(1));
		}
		if (!isDecimalComputation(dividend, divisor)) {
			return Optional.empty();
		}
		final DecimalType decimalType = LogicalTypeMerging.findDivisionDecimalType(
			getPrecision(dividend),
			getScale(dividend),
			getPrecision(divisor),
			getScale(divisor));
		return Optional.of(fromLogicalToDataType(decimalType));
	};

	/**
	 * Type strategy that returns the product of an exact numeric multiplication that includes at least
	 * one decimal.
	 */
	public static final TypeStrategy DECIMAL_TIMES = callContext -> {
		final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		final LogicalType factor1 = argumentDataTypes.get(0).getLogicalType();
		final LogicalType factor2 = argumentDataTypes.get(1).getLogicalType();
		// a hack to make legacy types possible until we drop them
		if (factor1 instanceof LegacyTypeInformationType) {
			return Optional.of(argumentDataTypes.get(0));
		}
		if (factor2 instanceof LegacyTypeInformationType) {
			return Optional.of(argumentDataTypes.get(1));
		}
		if (!isDecimalComputation(factor1, factor2)) {
			return Optional.empty();
		}
		final DecimalType decimalType = LogicalTypeMerging.findMultiplicationDecimalType(
			getPrecision(factor1),
			getScale(factor1),
			getPrecision(factor2),
			getScale(factor2));
		return Optional.of(fromLogicalToDataType(decimalType));
	};

	/**
	 * Type strategy that returns the modulo of an exact numeric division that includes at least
	 * one decimal.
	 */
	public static final TypeStrategy DECIMAL_MOD = callContext -> {
		final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		final LogicalType dividend = argumentDataTypes.get(0).getLogicalType();
		final LogicalType divisor = argumentDataTypes.get(1).getLogicalType();
		// a hack to make legacy types possible until we drop them
		if (dividend instanceof LegacyTypeInformationType) {
			return Optional.of(argumentDataTypes.get(0));
		}
		if (divisor instanceof LegacyTypeInformationType) {
			return Optional.of(argumentDataTypes.get(1));
		}
		if (!isDecimalComputation(dividend, divisor)) {
			return Optional.empty();
		}
		final int dividendScale = getScale(dividend);
		final int divisorScale = getScale(divisor);
		if (dividendScale == 0 && divisorScale == 0) {
			return Optional.of(argumentDataTypes.get(1));
		}
		final DecimalType decimalType = LogicalTypeMerging.findModuloDecimalType(
			getPrecision(dividend),
			dividendScale,
			getPrecision(divisor),
			divisorScale);
		return Optional.of(fromLogicalToDataType(decimalType));
	};

	/**
	 * Strategy that returns a decimal type but with a scale of 0.
	 */
	public static final TypeStrategy DECIMAL_SCALE0 = callContext -> {
		final DataType argumentDataType = callContext.getArgumentDataTypes().get(0);
		final LogicalType argumentType = argumentDataType.getLogicalType();
		// a hack to make legacy types possible until we drop them
		if (argumentType instanceof LegacyTypeInformationType) {
			return Optional.of(argumentDataType);
		}
		if (hasRoot(argumentType, LogicalTypeRoot.DECIMAL)) {
			if (hasScale(argumentType, 0)) {
				return Optional.of(argumentDataType);
			}
			final LogicalType inferredType = new DecimalType(argumentType.isNullable(), getPrecision(argumentType), 0);
			return Optional.of(fromLogicalToDataType(inferredType));
		}
		return Optional.empty();
	};

	/**
	 * Type strategy that returns the result of a rounding operation.
	 */
	public static final TypeStrategy ROUND = callContext -> {
		final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		final DataType argumentDataType = callContext.getArgumentDataTypes().get(0);
		final LogicalType argumentType = argumentDataType.getLogicalType();
		// a hack to make legacy types possible until we drop them
		if (argumentType instanceof LegacyTypeInformationType) {
			return Optional.of(argumentDataType);
		}
		if (!hasRoot(argumentType, LogicalTypeRoot.DECIMAL)) {
			return Optional.of(argumentDataType);
		}
		final BigDecimal roundLength;
		if (argumentDataTypes.size() == 2) {
			if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
				return Optional.of(argumentDataType);
			}
			roundLength = callContext.getArgumentValue(1, BigDecimal.class).orElseThrow(AssertionError::new);
		} else {
			roundLength = BigDecimal.ZERO;
		}
		final LogicalType inferredType = LogicalTypeMerging.findRoundDecimalType(
			getPrecision(argumentType),
			getScale(argumentType),
			roundLength.intValueExact());
		return Optional.of(fromLogicalToDataType(inferredType));
	};

	/**
	 * Type strategy that returns the type of a string concatenation. It assumes that the first two
	 * arguments are of the same family of either {@link LogicalTypeFamily#BINARY_STRING} or
	 * {@link LogicalTypeFamily#CHARACTER_STRING}.
	 */
	public static final TypeStrategy STRING_CONCAT = callContext -> {
		final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
		final LogicalType type1 = argumentDataTypes.get(0).getLogicalType();
		final LogicalType type2 = argumentDataTypes.get(1).getLogicalType();
		int length = getLength(type1) + getLength(type2);
		// handle overflow
		if (length < 0) {
			length = CharType.MAX_LENGTH;
		}
		final LogicalType minimumType;
		if (hasFamily(type1, LogicalTypeFamily.CHARACTER_STRING) || hasFamily(type2, LogicalTypeFamily.CHARACTER_STRING)) {
			minimumType = new CharType(false, length);
		} else if (hasFamily(type1, LogicalTypeFamily.BINARY_STRING) || hasFamily(type2, LogicalTypeFamily.BINARY_STRING)) {
			minimumType = new BinaryType(false, length);
		} else {
			return Optional.empty();
		}
		// deal with nullability handling and varying semantics
		return findCommonType(Arrays.asList(type1, type2, minimumType))
			.map(TypeConversions::fromLogicalToDataType);
	};

	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("BooleanMethodIsAlwaysInverted")
	private static boolean isDecimalComputation(LogicalType type1, LogicalType type2) {
		// both must be exact numeric
		if (!hasFamily(type1, LogicalTypeFamily.EXACT_NUMERIC) || !hasFamily(type2, LogicalTypeFamily.EXACT_NUMERIC)) {
			return false;
		}
		// one decimal must be present
		return hasRoot(type1, LogicalTypeRoot.DECIMAL) || hasRoot(type2, LogicalTypeRoot.DECIMAL);
	}

	private TypeStrategies() {
		// no instantiation
	}
}
