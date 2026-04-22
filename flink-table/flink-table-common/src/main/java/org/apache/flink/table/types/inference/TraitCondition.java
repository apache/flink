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

import java.util.Objects;

/**
 * A condition that determines whether a conditional trait on a {@link StaticArgument} should be
 * active for a given call.
 *
 * <p>Conditions are evaluated at planning time using the {@link TraitContext} which provides access
 * to the SQL call's properties (PARTITION BY presence, scalar literal values, etc.).
 *
 * <p>Implementations must implement {@code hashCode} and {@code equals} for {@link
 * StaticArgument#equals}/{@link StaticArgument#hashCode} to work correctly. The built-in factories
 * below return value-comparable instances; user-supplied lambdas do not - prefer the factories or
 * named classes.
 *
 * <p>Use the static factory methods for common conditions:
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
        return HasPartitionByCondition.INSTANCE;
    }

    /** True when the named scalar argument equals the expected value. */
    static <T> TraitCondition argIsEqualTo(final String name, final T expected) {
        return new ArgIsEqualToCondition<>(name, expected);
    }

    /** Negates the given condition. */
    static TraitCondition not(final TraitCondition condition) {
        return new NotCondition(condition);
    }

    // --------------------------------------------------------------------------------------------
    // Built-in implementations - named so that StaticArgument equality cascades correctly.
    // --------------------------------------------------------------------------------------------

    /** Singleton condition that is true when PARTITION BY is provided on the table argument. */
    final class HasPartitionByCondition implements TraitCondition {

        private static final HasPartitionByCondition INSTANCE = new HasPartitionByCondition();

        private HasPartitionByCondition() {}

        @Override
        public boolean test(final TraitContext ctx) {
            return ctx.hasPartitionBy();
        }

        // equals/hashCode by identity - safe because there is exactly one instance.
    }

    /** Condition that is true when the named scalar argument equals the expected value. */
    final class ArgIsEqualToCondition<T> implements TraitCondition {

        private final String name;
        private final T expected;
        private final Class<T> clazz;

        @SuppressWarnings("unchecked")
        ArgIsEqualToCondition(final String name, final T expected) {
            this.name = name;
            this.expected = expected;
            this.clazz = (Class<T>) expected.getClass();
        }

        @Override
        public boolean test(final TraitContext ctx) {
            return ctx.getScalarArgument(name, clazz).map(expected::equals).orElse(false);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ArgIsEqualToCondition)) {
                return false;
            }
            final ArgIsEqualToCondition<?> that = (ArgIsEqualToCondition<?>) o;
            return name.equals(that.name) && expected.equals(that.expected);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, expected);
        }
    }

    /** Condition that negates another condition. */
    final class NotCondition implements TraitCondition {

        private final TraitCondition condition;

        NotCondition(final TraitCondition condition) {
            this.condition = condition;
        }

        @Override
        public boolean test(final TraitContext ctx) {
            return !condition.test(ctx);
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof NotCondition)) {
                return false;
            }
            return condition.equals(((NotCondition) o).condition);
        }

        @Override
        public int hashCode() {
            return Objects.hash(NotCondition.class, condition);
        }
    }
}
