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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/** {@link InputTypeStrategy} for {@link BuiltInFunctionDefinitions#IN}. */
@Internal
public class SubQueryInputTypeStrategy implements InputTypeStrategy {
    @Override
    public ArgumentCount getArgumentCount() {
        return ConstantArgumentCount.from(2);
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final LogicalType rightType;
        final DataType leftType = callContext.getArgumentDataTypes().get(0);
        if (callContext.getArgumentDataTypes().size() > 2) {
            final Optional<LogicalType> commonType =
                    LogicalTypeMerging.findCommonType(
                            callContext.getArgumentDataTypes().stream()
                                    .map(DataType::getLogicalType)
                                    .collect(Collectors.toList()));
            if (!commonType.isPresent()) {
                return callContext.fail(
                        throwOnFailure, "Could not find a common type of the sublist.");
            }
            rightType = commonType.get();
        } else {
            rightType = callContext.getArgumentDataTypes().get(1).getLogicalType();
        }

        // check if the types are comparable, if the types are not comparable, check if it is not
        // a sub-query case like SELECT a IN (SELECT b FROM table1). We check if the result of the
        // rightType is of a ROW type with a single column, and if that column is comparable with
        // left type
        if (!LogicalTypeChecks.areComparable(
                        leftType.getLogicalType(),
                        rightType,
                        StructuredType.StructuredComparison.EQUALS)
                && !isComparableWithSubQuery(leftType.getLogicalType(), rightType)) {
            return callContext.fail(
                    throwOnFailure,
                    "Types on the right side of IN operator (%s) are not comparable with %s.",
                    rightType,
                    leftType.getLogicalType());
        }

        return Optional.of(
                Stream.concat(
                                Stream.of(leftType),
                                IntStream.range(1, callContext.getArgumentDataTypes().size())
                                        .mapToObj(
                                                i ->
                                                        TypeConversions.fromLogicalToDataType(
                                                                rightType)))
                        .collect(Collectors.toList()));
    }

    private static boolean isComparableWithSubQuery(LogicalType left, LogicalType right) {
        if (right.is(LogicalTypeRoot.ROW) && right.getChildren().size() == 1) {
            final RowType rowType = (RowType) right;
            return LogicalTypeChecks.areComparable(
                    left, rowType.getTypeAt(0), StructuredType.StructuredComparison.EQUALS);
        }
        return false;
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Arrays.asList(
                Signature.of(
                        Signature.Argument.ofGroup("COMPARABLE"),
                        Signature.Argument.ofGroupVarying("COMPARABLE")),
                Signature.of(
                        Signature.Argument.ofGroup("COMPARABLE"),
                        Signature.Argument.ofGroup("SUBQUERY")));
    }
}
