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

package org.apache.flink.table.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.delegation.PlannerTypeInferenceUtil;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;

import java.util.Optional;

/**
 * Catalog of functions that can resolve the name of a function to a {@link
 * ContextResolvedFunction}.
 */
@Internal
public interface FunctionLookup {

    /**
     * Lookup a function by function identifier. The identifier is parsed The lookup is case
     * insensitive.
     */
    Optional<ContextResolvedFunction> lookupFunction(String stringIdentifier);

    /** Lookup a function by function identifier. The lookup is case insensitive. */
    Optional<ContextResolvedFunction> lookupFunction(UnresolvedIdentifier identifier);

    /** Helper method for looking up a built-in function. */
    default ContextResolvedFunction lookupBuiltInFunction(BuiltInFunctionDefinition definition) {
        return lookupFunction(UnresolvedIdentifier.of(definition.getName()))
                .orElseThrow(
                        () ->
                                new TableException(
                                        String.format(
                                                "Required built-in function [%s] could not be found in any catalog.",
                                                definition.getName())));
    }

    /** Temporary utility until the new type inference is fully functional. */
    PlannerTypeInferenceUtil getPlannerTypeInferenceUtil();
}
