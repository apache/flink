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

import java.util.List;
import java.util.Optional;

/** Type strategy of {@code TO_TIMESTAMP_LTZ}. */
@Internal
public class ToTimestampLtzTypeStrategy implements TypeStrategy {

    private static final int DEFAULT_PRECISION = 3;

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

        LogicalType firstType = argumentTypes.get(0).getLogicalType();
        LogicalTypeRoot firstTypeRoot = firstType.getTypeRoot();

        if (argCount == 1) {
            if (!isCharacterType(firstTypeRoot) && !firstType.is(LogicalTypeFamily.NUMERIC)) {
                throw new ValidationException(
                        "Unsupported argument type. "
                                + "When taking 1 argument, TO_TIMESTAMP_LTZ accepts an argument of type <VARCHAR>, <CHAR>, or <NUMERIC>.");
            }
        } else if (argCount == 2) {
            LogicalType secondType = argumentTypes.get(1).getLogicalType();
            LogicalTypeRoot secondTypeRoot = secondType.getTypeRoot();
            if (firstType.is(LogicalTypeFamily.NUMERIC)) {
                if (secondTypeRoot != LogicalTypeRoot.INTEGER) {
                    throw new ValidationException(
                            "Unsupported argument type. "
                                    + "TO_TIMESTAMP_LTZ(<NUMERIC>, <INTEGER>) requires the second argument to be <INTEGER>.");
                }
            } else if (isCharacterType(firstTypeRoot)) {
                if (!isCharacterType(secondTypeRoot)) {
                    throw new ValidationException(
                            "Unsupported argument type. "
                                    + "If the first argument is of type <VARCHAR> or <CHAR>, TO_TIMESTAMP_LTZ requires the second argument to be of type <VARCHAR> or <CHAR>.");
                }
            } else {
                throw new ValidationException(
                        "Unsupported argument type. "
                                + "When taking 2 arguments, TO_TIMESTAMP_LTZ requires the first argument to be of type <VARCHAR>, <CHAR>, or <NUMERIC>.");
            }
        } else if (argCount == 3) {
            if (!isCharacterType(firstTypeRoot)
                    || !isCharacterType(argumentTypes.get(1).getLogicalType().getTypeRoot())
                    || !isCharacterType(argumentTypes.get(2).getLogicalType().getTypeRoot())) {
                throw new ValidationException(
                        "Unsupported argument type. "
                                + "When taking 3 arguments, TO_TIMESTAMP_LTZ requires all three arguments to be of type <VARCHAR> or <CHAR>.");
            }
        }

        return Optional.of(DataTypes.TIMESTAMP_LTZ(DEFAULT_PRECISION).nullable());
    }

    private boolean isCharacterType(LogicalTypeRoot typeRoot) {
        return typeRoot == LogicalTypeRoot.CHAR || typeRoot == LogicalTypeRoot.VARCHAR;
    }
}
