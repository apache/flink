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

import java.util.Optional;

/** Strategy for inferring and validating a single input argument type of a function call. */
@PublicEvolving
public interface ArgumentTypeStrategy {

    /**
     * Main logic for inferring and validating an argument. Returns the data type that is valid for
     * the given call. If the returned type differs from {@link CallContext#getArgumentDataTypes()}
     * at {@code argumentPos}, a casting operation can be inserted. An empty result means that the
     * given input type could not be inferred.
     *
     * @param callContext provides details about the function call
     * @param argumentPos argument index in the {@link CallContext}
     * @param throwOnFailure whether this function is allowed to throw an {@link
     *     ValidationException} with a meaningful exception in case the inference is not successful
     *     or if this function should simply return an empty result.
     * @return three-state result for either "true, same data type as argument", "true, but argument
     *     must be casted to returned data type", or "false, no inferred data type could be found"
     * @see CallContext#newValidationError(String, Object...)
     */
    Optional<DataType> inferArgumentType(
            CallContext callContext, int argumentPos, boolean throwOnFailure);

    /**
     * Returns a summary of the function's expected argument at {@code argumentPos}.
     *
     * @param functionDefinition the function definition that defines the function currently being
     *     called.
     * @param argumentPos the position within the function call for which the signature should be
     *     retrieved
     */
    Signature.Argument getExpectedArgument(FunctionDefinition functionDefinition, int argumentPos);
}
