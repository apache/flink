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
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.StructuredType.StructuredComparison;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * An {@link InputTypeStrategy} that checks if all input arguments can be compared with each other
 * with the minimal provided comparison.
 *
 * <p>It requires at least one argument. In case of one argument, the argument must be comparable
 * with itself (e.g. for aggregations).
 *
 * <p>For the rules which types are comparable with which types see {@link
 * LogicalTypeChecks#areComparable(LogicalType, LogicalType, StructuredComparison)}.
 */
@Internal
public final class ComparableTypeStrategy implements InputTypeStrategy {
    private final StructuredComparison requiredComparison;
    private final ConstantArgumentCount argumentCount;

    public ComparableTypeStrategy(
            ConstantArgumentCount argumentCount, StructuredComparison requiredComparison) {
        Preconditions.checkArgument(
                argumentCount.getMinCount().map(c -> c >= 1).orElse(false),
                "Comparable type strategy requires at least one argument. Actual minimal argument count: %s",
                argumentCount.getMinCount().map(Objects::toString).orElse("<None>"));
        Preconditions.checkArgument(requiredComparison != StructuredComparison.NONE);
        this.requiredComparison = requiredComparison;
        this.argumentCount = argumentCount;
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return argumentCount;
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        if (argumentDataTypes.size() == 1) {
            final LogicalType argType = argumentDataTypes.get(0).getLogicalType();
            if (!LogicalTypeChecks.areComparable(argType, argType, requiredComparison)) {
                return callContext.fail(
                        throwOnFailure,
                        "Type '%s' should support %s comparison with itself.",
                        argType,
                        comparisonToString());
            }
        } else {
            for (int i = 0; i < argumentDataTypes.size() - 1; i++) {
                final LogicalType firstType = argumentDataTypes.get(i).getLogicalType();
                final LogicalType secondType = argumentDataTypes.get(i + 1).getLogicalType();

                if (!LogicalTypeChecks.areComparable(firstType, secondType, requiredComparison)) {
                    return callContext.fail(
                            throwOnFailure,
                            "All types in a comparison should support %s comparison with each other. "
                                    + "Can not compare %s with %s",
                            comparisonToString(),
                            firstType,
                            secondType);
                }
            }
        }

        return Optional.of(argumentDataTypes);
    }

    private String comparisonToString() {
        return requiredComparison == StructuredComparison.EQUALS
                ? "'EQUALS'"
                : "both 'EQUALS' and 'ORDER'";
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(
                Signature.of(Signature.Argument.ofGroupVarying("COMPARABLE")));
    }
}
