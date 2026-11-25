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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.inference.TraitCondition;

import java.util.Arrays;
import java.util.Map;

/**
 * Trait conditions of {@link BuiltInFunctionDefinitions#TO_CHANGELOG}. They inspect the {@code
 * op_mapping} and {@code produces_full_deletes} arguments to decide which change operations the
 * function forwards.
 */
@Internal
final class ToChangelogDefinition {

    /**
     * True when the function forwards UPDATE_BEFORE rows: {@code op_mapping} maps UPDATE_BEFORE or
     * is omitted, as the default mapping includes UPDATE_BEFORE.
     */
    static final TraitCondition FORWARDS_UPDATE_BEFORE =
            TraitCondition.or(
                    TraitCondition.not(TraitCondition.argIsPresent("op_mapping")),
                    opMappingContains("UPDATE_BEFORE"));

    /**
     * True when the function forwards full DELETE rows: {@code op_mapping} maps DELETE or is
     * omitted, and {@code produces_full_deletes} is not FALSE. Without a mapped DELETE, no DELETE
     * rows reach the output, so full deletes are not needed.
     */
    static final TraitCondition FORWARDS_FULL_DELETES =
            TraitCondition.and(
                    TraitCondition.or(
                            TraitCondition.not(
                                    TraitCondition.argIsPresent("produces_full_deletes")),
                            TraitCondition.argIsEqualTo("produces_full_deletes", Boolean.TRUE)),
                    TraitCondition.or(
                            TraitCondition.not(TraitCondition.argIsPresent("op_mapping")),
                            opMappingContains("DELETE")));

    /**
     * True when the function forwards only INSERT and UPDATE_AFTER rows: {@code op_mapping} has no
     * entry for UPDATE_BEFORE and DELETE.
     */
    static final TraitCondition FORWARDS_NO_RETRACTIONS =
            TraitCondition.argMatches(
                    "op_mapping",
                    Map.class,
                    mapping ->
                            !containsChangeOperation(mapping, "UPDATE_BEFORE")
                                    && !containsChangeOperation(mapping, "DELETE"));

    private static TraitCondition opMappingContains(final String changeOperation) {
        return TraitCondition.argMatches(
                "op_mapping",
                Map.class,
                mapping -> containsChangeOperation(mapping, changeOperation));
    }

    /**
     * True when {@code changeOperation} appears among the {@code op_mapping} keys. Each map key may
     * itself be a comma-separated list (e.g. {@code "INSERT,UPDATE_AFTER"}) - each part is trimmed
     * and compared against {@code changeOperation}, so one entry can cover multiple change kinds.
     */
    private static boolean containsChangeOperation(
            final Map<?, ?> opMapping, final String changeOperation) {
        return opMapping.keySet().stream()
                .flatMap(key -> Arrays.stream(((String) key).split(",")))
                .map(String::trim)
                .anyMatch(changeOperation::equals);
    }

    private ToChangelogDefinition() {}
}
