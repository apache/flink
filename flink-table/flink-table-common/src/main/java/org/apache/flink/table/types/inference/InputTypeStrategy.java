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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Optional;

/**
 * Strategy for inferring and validating input arguments in a function call.
 *
 * <p>This interface has three responsibilities during the {@link TypeInference} process:
 *
 * <p>It can help in resolving the type of untyped {@code NULL} literals.
 *
 * <p>It validates the types of the input arguments.
 *
 * <p>During the planning process, it can help in resolving the complete {@link DataType}, i.e., the
 * conversion class that a function implementation expects from the runtime. This requires that a
 * strategy can also be called on already validated arguments without affecting the logical type.
 *
 * <p>Note: Implementations should implement {@link Object#hashCode()} and {@link
 * Object#equals(Object)}.
 *
 * @see InputTypeStrategies
 */
@PublicEvolving
public interface InputTypeStrategy {

    /** Initial input validation based on the number of arguments. */
    ArgumentCount getArgumentCount();

    /**
     * Main logic for inferring and validating the input arguments. Returns a list of argument data
     * types that are valid for the given call. If the returned types differ from {@link
     * CallContext#getArgumentDataTypes()}, a casting operation can be inserted. An empty result
     * means that the given input is invalid.
     *
     * @param callContext provides details about the function call
     * @param throwOnFailure whether this function is allowed to throw an {@link
     *     ValidationException} with a meaningful exception in case the inference is not successful
     *     or if this function should simply return an empty result.
     * @return three-state result for either "true, same data types as arguments", "true, but
     *     arguments must be casted to returned data types", or "false, no inferred data types could
     *     be found"
     * @see CallContext#newValidationError(String, Object...)
     */
    Optional<List<DataType>> inferInputTypes(CallContext callContext, boolean throwOnFailure);

    /**
     * Returns a summary of the function's expected signatures.
     *
     * @param definition the function definition that defines the function currently being called.
     */
    List<Signature> getExpectedSignatures(FunctionDefinition definition);
}
