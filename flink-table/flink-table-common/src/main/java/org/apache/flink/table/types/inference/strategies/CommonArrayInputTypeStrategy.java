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
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.inference.Signature.Argument;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.table.types.utils.TypeConversions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** An {@link InputTypeStrategy} that expects that all arguments have a common array type. */
@Internal
public final class CommonArrayInputTypeStrategy implements InputTypeStrategy {

    private static final Argument COMMON_ARGUMENT = Argument.ofGroup("COMMON");

    private final ArgumentCount argumentCount;

    public CommonArrayInputTypeStrategy(ArgumentCount argumentCount) {
        this.argumentCount = argumentCount;
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return argumentCount;
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        List<DataType> argumentDataTypes = callContext.getArgumentDataTypes();
        List<LogicalType> argumentTypes =
                argumentDataTypes.stream()
                        .map(DataType::getLogicalType)
                        .collect(Collectors.toList());

        if (!argumentTypes.stream()
                .allMatch(logicalType -> logicalType.is(LogicalTypeRoot.ARRAY))) {
            return callContext.fail(throwOnFailure, "All arguments requires to be a ARRAY type");
        }

        if (argumentTypes.stream().anyMatch(CommonArrayInputTypeStrategy::isLegacyType)) {
            return Optional.of(argumentDataTypes);
        }

        Optional<LogicalType> commonType = LogicalTypeMerging.findCommonType(argumentTypes);

        if (!commonType.isPresent()) {
            return callContext.fail(
                    throwOnFailure,
                    "Could not find a common type for arguments: %s",
                    argumentDataTypes);
        }

        return commonType.map(
                type ->
                        Collections.nCopies(
                                argumentTypes.size(), TypeConversions.fromLogicalToDataType(type)));
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        Optional<Integer> minCount = argumentCount.getMinCount();
        Optional<Integer> maxCount = argumentCount.getMaxCount();

        int numberOfMandatoryArguments = minCount.orElse(0);

        if (maxCount.isPresent()) {
            return IntStream.range(numberOfMandatoryArguments, maxCount.get() + 1)
                    .mapToObj(count -> Signature.of(Collections.nCopies(count, COMMON_ARGUMENT)))
                    .collect(Collectors.toList());
        }

        List<Argument> arguments =
                new ArrayList<>(Collections.nCopies(numberOfMandatoryArguments, COMMON_ARGUMENT));
        arguments.add(Argument.ofGroupVarying("COMMON"));
        return Collections.singletonList(Signature.of(arguments));
    }

    private static boolean isLegacyType(LogicalType type) {
        return type instanceof LegacyTypeInformationType;
    }
}
