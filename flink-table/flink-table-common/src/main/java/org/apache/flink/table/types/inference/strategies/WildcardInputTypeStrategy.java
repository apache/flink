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
import org.apache.flink.table.types.inference.Signature.Argument;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Strategy that does not perform any modification or validation of the input. */
@Internal
public final class WildcardInputTypeStrategy implements InputTypeStrategy {

    private static final ArgumentCount PASSING_ARGUMENT_COUNT = ConstantArgumentCount.any();
    private final ArgumentCount argumentCount;

    public WildcardInputTypeStrategy(ArgumentCount argumentCount) {
        this.argumentCount = argumentCount;
    }

    public WildcardInputTypeStrategy() {
        this(PASSING_ARGUMENT_COUNT);
    }

    @Override
    public ArgumentCount getArgumentCount() {
        return argumentCount;
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        return Optional.of(callContext.getArgumentDataTypes());
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        return Collections.singletonList(Signature.of(Argument.of("*")));
    }

    @Override
    public boolean equals(Object o) {
        return this == o || o instanceof WildcardInputTypeStrategy;
    }

    @Override
    public int hashCode() {
        return WildcardInputTypeStrategy.class.hashCode();
    }
}
