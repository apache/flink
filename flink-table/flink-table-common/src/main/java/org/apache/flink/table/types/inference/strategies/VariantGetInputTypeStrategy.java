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
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.utils.SimplePath;

import java.util.List;
import java.util.Optional;

import static org.apache.flink.table.types.inference.InputTypeStrategies.LITERAL;
import static org.apache.flink.table.types.inference.InputTypeStrategies.and;
import static org.apache.flink.table.types.inference.InputTypeStrategies.logical;
import static org.apache.flink.table.types.inference.InputTypeStrategies.or;
import static org.apache.flink.table.types.inference.InputTypeStrategies.sequence;
import static org.apache.flink.table.types.inference.strategies.VariantGetTypeStrategy.ARG_RETURN_TYPE;

/**
 * Input strategy for {@code VARIANT_GET(variant, path [, returnType])} and {@code TRY_VARIANT_GET}.
 *
 * <p>A literal path must be a {@link SimplePath}, so a malformed one fails at planning time. A path
 * that is not a literal is parsed when the function runs. The {@code returnType} must be a literal
 * that names a type {@code VARIANT} can be cast to, see {@link VariantGetTypeStrategy}.
 */
@Internal
public final class VariantGetInputTypeStrategy implements InputTypeStrategy {

    private static final int ARG_PATH = 1;

    private static final InputTypeStrategy SIGNATURES =
            or(
                    sequence(
                            List.of("variant", "path"),
                            List.of(
                                    logical(LogicalTypeRoot.VARIANT),
                                    logical(LogicalTypeFamily.CHARACTER_STRING))),
                    sequence(
                            List.of("variant", "path", "returnType"),
                            List.of(
                                    logical(LogicalTypeRoot.VARIANT),
                                    logical(LogicalTypeFamily.CHARACTER_STRING),
                                    and(logical(LogicalTypeFamily.CHARACTER_STRING), LITERAL))));

    @Override
    public ArgumentCount getArgumentCount() {
        return SIGNATURES.getArgumentCount();
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            final CallContext callContext, final boolean throwOnFailure) {
        final Optional<List<DataType>> inferredDataTypes =
                SIGNATURES.inferInputTypes(callContext, throwOnFailure);
        if (inferredDataTypes.isEmpty()) {
            return inferredDataTypes;
        }

        if (callContext.isArgumentLiteral(ARG_PATH)) {
            final Optional<String> path = callContext.getArgumentValue(ARG_PATH, String.class);
            if (path.isPresent()) {
                try {
                    SimplePath.parse(path.get());
                } catch (ValidationException e) {
                    return callContext.fail(throwOnFailure, e.getMessage());
                }
            }
        }

        if (callContext.getArgumentDataTypes().size() > ARG_RETURN_TYPE) {
            try {
                VariantGetTypeStrategy.resolveTargetType(callContext);
            } catch (ValidationException e) {
                return callContext.fail(throwOnFailure, e.getMessage());
            }
        }

        return inferredDataTypes;
    }

    @Override
    public List<Signature> getExpectedSignatures(final FunctionDefinition definition) {
        return SIGNATURES.getExpectedSignatures(definition);
    }
}
