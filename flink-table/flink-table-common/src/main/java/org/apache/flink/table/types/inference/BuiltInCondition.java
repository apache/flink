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

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Internal value-comparable wrapper used by all built-in {@link TraitCondition} factories. Equality
 * is keyed by {@code kind + args}; the {@code impl} predicate is reused but never compared, so two
 * conditions built from the same factory inputs are equal.
 *
 * <p>Lives outside {@link TraitCondition} because Java forbids {@code private} nested types in
 * interfaces (they are implicitly {@code public static}); top-level package-private gives the same
 * encapsulation.
 */
final class BuiltInCondition implements TraitCondition {

    /** Tag identifying which factory produced the condition. */
    enum Kind {
        HAS_PARTITION_BY,
        ARG_IS_EQUAL_TO,
        NOT
    }

    private final Kind kind;
    private final List<Object> args;
    private final Predicate<TraitContext> impl;

    BuiltInCondition(final Kind kind, final List<Object> args, final Predicate<TraitContext> impl) {
        this.kind = kind;
        this.args = args;
        this.impl = impl;
    }

    @Override
    public boolean test(final TraitContext ctx) {
        return impl.test(ctx);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BuiltInCondition)) {
            return false;
        }
        final BuiltInCondition that = (BuiltInCondition) o;
        return kind == that.kind && args.equals(that.args);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, args);
    }
}
