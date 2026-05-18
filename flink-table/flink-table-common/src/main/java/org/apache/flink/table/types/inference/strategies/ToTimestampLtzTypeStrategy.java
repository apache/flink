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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.DateTimeUtils;

import java.util.List;
import java.util.Optional;

/** Type strategy of {@code TO_TIMESTAMP_LTZ}. */
@Internal
public class ToTimestampLtzTypeStrategy implements TypeStrategy {

    private static final int MIN_PRECISION = 0;
    private static final int MAX_PRECISION = 9;
    private static final int DEFAULT_PRECISION = 3;

    private static final int EPOCH_OR_TIMESTAMP_ARG = 0;
    private static final int PRECISION_OR_FORMAT_ARG = 1;
    private static final int TIMEZONE_ARG = 2;

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        List<DataType> argumentTypes = callContext.getArgumentDataTypes();
        int argCount = argumentTypes.size();

        if (argCount < 1 || argCount > 3) {
            throw new ValidationException(
                    "Unsupported argument type. "
                            + "TO_TIMESTAMP_LTZ requires 1 to 3 arguments, but "
                            + argCount
                            + " were provided.");
        }

        LogicalType firstType = argumentTypes.get(EPOCH_OR_TIMESTAMP_ARG).getLogicalType();
        LogicalTypeRoot firstTypeRoot = firstType.getTypeRoot();
        int outputPrecision = DEFAULT_PRECISION;

        switch (argCount) {
            case 1:
                if (!isCharacterType(firstTypeRoot) && !firstType.is(LogicalTypeFamily.NUMERIC)) {
                    throw new ValidationException(
                            "Unsupported argument type. "
                                    + "When taking 1 argument, TO_TIMESTAMP_LTZ accepts an argument of type <VARCHAR>, <CHAR>, or <NUMERIC>.");
                }
                break;
            case 2:
                LogicalType secondType =
                        argumentTypes.get(PRECISION_OR_FORMAT_ARG).getLogicalType();
                LogicalTypeRoot secondTypeRoot = secondType.getTypeRoot();
                if (firstType.is(LogicalTypeFamily.NUMERIC)) {
                    if (secondTypeRoot != LogicalTypeRoot.INTEGER) {
                        throw new ValidationException(
                                "Unsupported argument type. "
                                        + "TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>) requires the second argument to be <INTEGER>.");
                    }
                    outputPrecision = inferPrecisionFromIntegerArg(callContext);
                } else if (isCharacterType(firstTypeRoot)) {
                    if (!isCharacterType(secondTypeRoot)) {
                        throw new ValidationException(
                                "Unsupported argument type. "
                                        + "If the first argument is of type <VARCHAR> or <CHAR>, TO_TIMESTAMP_LTZ requires the second argument to be of type <VARCHAR> or <CHAR>.");
                    }
                    outputPrecision = inferPrecisionFromFormat(callContext);
                } else {
                    throw new ValidationException(
                            "Unsupported argument type. "
                                    + "When taking 2 arguments, TO_TIMESTAMP_LTZ requires the first argument to be of type <VARCHAR>, <CHAR>, or <NUMERIC>.");
                }
                break;
            case 3:
                if (!isCharacterType(firstTypeRoot)
                        || !isCharacterType(
                                argumentTypes
                                        .get(PRECISION_OR_FORMAT_ARG)
                                        .getLogicalType()
                                        .getTypeRoot())
                        || !isCharacterType(
                                argumentTypes.get(TIMEZONE_ARG).getLogicalType().getTypeRoot())) {
                    throw new ValidationException(
                            "Unsupported argument type. "
                                    + "When taking 3 arguments, TO_TIMESTAMP_LTZ requires all three arguments to be of type <VARCHAR> or <CHAR>.");
                }
                outputPrecision = inferPrecisionFromFormat(callContext);
        }

        return Optional.of(DataTypes.TIMESTAMP_LTZ(outputPrecision).nullable());
    }

    /**
     * Infers the output precision from a precision integer literal.
     *
     * <p>Same plan-time literal constraint as {@link #inferPrecisionFromFormat(CallContext)}: when
     * the precision argument is a non-literal expression, the output defaults to {@link
     * #DEFAULT_PRECISION}.
     *
     * @return precision in [{@link #DEFAULT_PRECISION}, {@link #MAX_PRECISION}]
     */
    private static int inferPrecisionFromIntegerArg(CallContext callContext) {
        if (!callContext.isArgumentLiteral(PRECISION_OR_FORMAT_ARG)) {
            return DEFAULT_PRECISION;
        }
        return callContext
                .getArgumentValue(PRECISION_OR_FORMAT_ARG, Integer.class)
                .map(
                        precision -> {
                            validatePrecision(precision);
                            return Math.max(precision, DEFAULT_PRECISION);
                        })
                .orElse(DEFAULT_PRECISION);
    }

    /**
     * Infers the output precision from a format string literal.
     *
     * <p>The output type must be deterministic at plan time, so this method can only inspect the
     * format pattern when it is a literal. When the format is a non-literal expression (e.g., a
     * column reference) the pattern is unknown until runtime and could vary per row, so we fall
     * back to {@link #DEFAULT_PRECISION}. The runtime still parses with the actual row pattern, but
     * any sub-millisecond digits are truncated by the implicit cast to the declared type.
     *
     * @return precision in [{@link #DEFAULT_PRECISION}, {@link #MAX_PRECISION}]
     */
    private static int inferPrecisionFromFormat(CallContext callContext) {
        if (!callContext.isArgumentLiteral(PRECISION_OR_FORMAT_ARG)) {
            return DEFAULT_PRECISION;
        }
        return callContext
                .getArgumentValue(PRECISION_OR_FORMAT_ARG, String.class)
                .map(DateTimeUtils::precisionFromFormat)
                .orElse(DEFAULT_PRECISION);
    }

    private static void validatePrecision(final int precision) {
        if (precision < MIN_PRECISION || precision > MAX_PRECISION) {
            throw new ValidationException(
                    String.format(
                            "Precision for TO_TIMESTAMP_LTZ must be between %d and %d but was %d.",
                            MIN_PRECISION, MAX_PRECISION, precision));
        }
    }

    private boolean isCharacterType(LogicalTypeRoot typeRoot) {
        return typeRoot == LogicalTypeRoot.CHAR || typeRoot == LogicalTypeRoot.VARCHAR;
    }
}
