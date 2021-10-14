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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getLength;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasFamily;
import static org.apache.flink.table.types.logical.utils.LogicalTypeMerging.findCommonType;

/**
 * Type strategy that returns the type of a string concatenation. It assumes that the first two
 * arguments are of the same family of either {@link LogicalTypeFamily#BINARY_STRING} or {@link
 * LogicalTypeFamily#CHARACTER_STRING}.
 */
@Internal
class StringConcatTypeStrategy implements TypeStrategy {

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        final LogicalType type1 = argumentDataTypes.get(0).getLogicalType();
        final LogicalType type2 = argumentDataTypes.get(1).getLogicalType();
        int length = getLength(type1) + getLength(type2);

        // handle overflow
        if (length < 0) {
            length = CharType.MAX_LENGTH;
        }

        final LogicalType minimumType;
        if (hasFamily(type1, LogicalTypeFamily.CHARACTER_STRING)
                || hasFamily(type2, LogicalTypeFamily.CHARACTER_STRING)) {
            minimumType = new CharType(false, length);
        } else if (hasFamily(type1, LogicalTypeFamily.BINARY_STRING)
                || hasFamily(type2, LogicalTypeFamily.BINARY_STRING)) {
            minimumType = new BinaryType(false, length);
        } else {
            return Optional.empty();
        }

        // deal with nullability handling and varying semantics
        return findCommonType(Arrays.asList(type1, type2, minimumType))
                .map(TypeConversions::fromLogicalToDataType);
    }
}
