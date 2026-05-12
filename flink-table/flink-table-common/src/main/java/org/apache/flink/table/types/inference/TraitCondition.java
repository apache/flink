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

import java.util.List;

/**
 * A condition that determines whether a conditional trait on a {@link StaticArgument} should be
 * active for a given call.
 *
 * <p>Conditions are evaluated at planning time using the {@link TraitContext} which provides access
 * to the SQL call's properties (PARTITION BY presence, scalar literal values, etc.).
 *
 * <p>Implementations must implement {@code hashCode} and {@code equals} for {@link
 * StaticArgument#equals}/{@link StaticArgument#hashCode} to work correctly. The built-in factories
 * below return value-comparable instances; user-supplied lambdas do not - prefer the factories.
 *
 * <pre>{@code
 * import static org.apache.flink.table.types.inference.TraitCondition.*;
 *
 * StaticArgument.table("input", Row.class, false, EnumSet.of(TABLE, SUPPORT_UPDATES))
 *         .withConditionalTrait(SET_SEMANTIC_TABLE, hasPartitionBy());
 * }</pre>
 */
@PublicEvolving
@FunctionalInterface
public interface TraitCondition {

    /** Evaluates this condition against the given context. */
    boolean test(TraitContext ctx);

    /** True when PARTITION BY is provided on the table argument. */
    static TraitCondition hasPartitionBy() {
        return new BuiltInCondition(
                BuiltInCondition.Kind.HAS_PARTITION_BY, List.of(), TraitContext::hasPartitionBy);
    }

    /** True when the named scalar argument equals the expected value. */
    @SuppressWarnings("unchecked")
    static <T> TraitCondition argIsEqualTo(final String name, final T expected) {
        final Class<T> clazz = (Class<T>) expected.getClass();
        return new BuiltInCondition(
                BuiltInCondition.Kind.ARG_IS_EQUAL_TO,
                List.of(name, expected),
                ctx -> ctx.getScalarArgument(name, clazz).map(expected::equals).orElse(false));
    }

    /** Negates the given condition. */
    static TraitCondition not(final TraitCondition condition) {
        return new BuiltInCondition(
                BuiltInCondition.Kind.NOT, List.of(condition), ctx -> !condition.test(ctx));
    }
}
