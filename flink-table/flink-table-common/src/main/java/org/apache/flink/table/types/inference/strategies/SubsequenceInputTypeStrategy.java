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
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An {@link InputTypeStrategy} that lets you apply other strategies for subsequences of the actual
 * arguments.
 *
 * <p>The {@link SequenceInputTypeStrategy} should be preferred in most of the cases. Use this
 * strategy only if you need to apply a common logic to a subsequence of the arguments.
 */
@Internal
public final class SubsequenceInputTypeStrategy implements InputTypeStrategy {

    private final List<ArgumentsSplit> argumentsSplits;
    private final ConstantArgumentCount argumentCount;

    private SubsequenceInputTypeStrategy(
            List<ArgumentsSplit> argumentsSplits, ConstantArgumentCount argumentCount) {
        this.argumentsSplits = Preconditions.checkNotNull(argumentsSplits);
        this.argumentCount = Preconditions.checkNotNull(argumentCount);
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return argumentCount;
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {

        List<DataType> result = new ArrayList<>();

        for (ArgumentsSplit argumentsSplit : argumentsSplits) {
            Optional<List<DataType>> splitDataTypes =
                    argumentsSplit.inputTypeStrategy.inferInputTypes(
                            new SplitCallContext(argumentsSplit, callContext), throwOnFailure);

            if (splitDataTypes.isPresent()) {
                result.addAll(splitDataTypes.get());
            } else {
                if (throwOnFailure) {
                    throw callContext.newValidationError(
                            "Could not infer arguments in range: [%d, %d].",
                            argumentsSplit.startIndex, argumentsSplit.endIndex);
                }
                return Optional.empty();
            }
        }

        return Optional.of(result);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return argumentsSplits.stream()
                .map(computeSubSignatures(definition))
                .reduce(this::crossJoin)
                .get()
                .stream()
                .map(Signature::of)
                .collect(Collectors.toList());
    }

    private Function<ArgumentsSplit, List<List<Signature.Argument>>> computeSubSignatures(
            FunctionDefinition definition) {
        return argumentsSplit ->
                argumentsSplit.inputTypeStrategy.getExpectedSignatures(definition).stream()
                        .map(Signature::getArguments)
                        .collect(Collectors.toList());
    }

    private List<List<Signature.Argument>> crossJoin(
            List<List<Signature.Argument>> signatureStarts,
            List<List<Signature.Argument>> signatureEnds) {
        return signatureStarts.stream()
                .flatMap(
                        x ->
                                signatureEnds.stream()
                                        .map(
                                                y -> {
                                                    ArrayList<Signature.Argument> joined =
                                                            new ArrayList<>(x);
                                                    joined.addAll(y);
                                                    return joined;
                                                }))
                .collect(Collectors.toList());
    }

    private static final class ArgumentsSplit {
        private final int startIndex;
        private final @Nullable Integer endIndex;

        private final InputTypeStrategy inputTypeStrategy;

        public ArgumentsSplit(
                int startIndex, Integer endIndex, InputTypeStrategy inputTypeStrategy) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.inputTypeStrategy = inputTypeStrategy;
        }
    }

    private static final class SplitCallContext implements CallContext {

        private final ArgumentsSplit split;
        private final CallContext originalCallContext;

        private SplitCallContext(ArgumentsSplit split, CallContext originalCallContext) {
            this.split = split;
            this.originalCallContext = originalCallContext;
        }

        @Override
        public DataTypeFactory getDataTypeFactory() {
            return originalCallContext.getDataTypeFactory();
        }

        @Override
        public FunctionDefinition getFunctionDefinition() {
            return originalCallContext.getFunctionDefinition();
        }

        @Override
        public boolean isArgumentLiteral(int pos) {
            return originalCallContext.isArgumentLiteral(pos + split.startIndex);
        }

        @Override
        public boolean isArgumentNull(int pos) {
            return originalCallContext.isArgumentLiteral(pos + split.startIndex);
        }

        @Override
        public <T> Optional<T> getArgumentValue(int pos, Class<T> clazz) {
            return originalCallContext.getArgumentValue(pos + split.startIndex, clazz);
        }

        @Override
        public String getName() {
            return originalCallContext.getName();
        }

        @Override
        public List<DataType> getArgumentDataTypes() {
            List<DataType> originalArgumentDataTypes = originalCallContext.getArgumentDataTypes();
            return originalArgumentDataTypes.subList(
                    split.startIndex,
                    split.endIndex != null ? split.endIndex : originalArgumentDataTypes.size());
        }

        @Override
        public Optional<DataType> getOutputDataType() {
            return originalCallContext.getOutputDataType();
        }

        @Override
        public boolean isGroupedAggregation() {
            return originalCallContext.isGroupedAggregation();
        }
    }

    /** A Builder for {@link SubsequenceInputTypeStrategy}. */
    public static final class SubsequenceStrategyBuilder {
        private final List<ArgumentsSplit> argumentsSplits = new ArrayList<>();
        private int currentPos = 0;

        /** Defines that we expect a single argument at the next position. */
        public SubsequenceStrategyBuilder argument(ArgumentTypeStrategy argumentTypeStrategy) {
            SequenceInputTypeStrategy singleArgumentStrategy =
                    new SequenceInputTypeStrategy(
                            Collections.singletonList(argumentTypeStrategy), null);
            argumentsSplits.add(
                    new ArgumentsSplit(currentPos, currentPos + 1, singleArgumentStrategy));
            currentPos += 1;
            return this;
        }

        /**
         * Defines a common {@link InputTypeStrategy} for the next arguments. Given input strategy
         * must expect a constant number of arguments. That means that both the minimum and maximum
         * number of arguments must be defined and equal to each other.
         *
         * <p>If you need a varying logic use {@link #finishWithVarying(InputTypeStrategy)}.
         */
        public SubsequenceStrategyBuilder subsequence(InputTypeStrategy inputTypeStrategy) {
            Preconditions.checkArgument(
                    inputTypeStrategy.getArgumentCount() instanceof ConstantArgumentCount);
            Optional<Integer> maxCount = inputTypeStrategy.getArgumentCount().getMaxCount();
            Optional<Integer> minCount = inputTypeStrategy.getArgumentCount().getMinCount();
            if (!maxCount.isPresent()
                    || !minCount.isPresent()
                    || !maxCount.get().equals(minCount.get())) {
                throw new IllegalArgumentException(
                        "Both the minimum and maximum number of expected arguments must"
                                + " be defined and equal to each other.");
            }
            argumentsSplits.add(
                    new ArgumentsSplit(currentPos, currentPos + maxCount.get(), inputTypeStrategy));
            currentPos += maxCount.get();
            return this;
        }

        /**
         * Defines a common {@link InputTypeStrategy} for the next arguments. Given input strategy
         * must expect a varying number of arguments. That means that the maximum number of
         * arguments must not be defined.
         */
        public InputTypeStrategy finishWithVarying(InputTypeStrategy inputTypeStrategy) {
            Preconditions.checkArgument(
                    inputTypeStrategy.getArgumentCount() instanceof ConstantArgumentCount);
            ArgumentCount strategyArgumentCount = inputTypeStrategy.getArgumentCount();
            strategyArgumentCount
                    .getMaxCount()
                    .ifPresent(
                            c -> {
                                throw new IllegalArgumentException(
                                        "The maximum number of arguments must not be defined.");
                            });
            argumentsSplits.add(new ArgumentsSplit(currentPos, null, inputTypeStrategy));
            int minCount = currentPos + strategyArgumentCount.getMinCount().orElse(0);
            return new SubsequenceInputTypeStrategy(
                    argumentsSplits, ConstantArgumentCount.from(minCount));
        }

        /** Constructs the given strategy. */
        public InputTypeStrategy finish() {
            return new SubsequenceInputTypeStrategy(
                    argumentsSplits, ConstantArgumentCount.of(currentPos));
        }
    }
}
