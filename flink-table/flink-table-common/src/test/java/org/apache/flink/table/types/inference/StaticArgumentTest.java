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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link StaticArgument}. */
class StaticArgumentTest {

    @Test
    void defaultTableArgIsKeyMode() {
        final StaticArgument arg =
                StaticArgument.table(
                        "in",
                        Row.class,
                        false,
                        EnumSet.of(
                                StaticArgumentTrait.TABLE, StaticArgumentTrait.SET_SEMANTIC_TABLE));
        assertThat(arg.getPassThroughMode()).isEqualTo(PassThroughMode.KEY);
    }

    @Test
    void rowSemanticTableArgIsKeyMode() {
        final StaticArgument arg =
                StaticArgument.table(
                        "in",
                        Row.class,
                        false,
                        EnumSet.of(
                                StaticArgumentTrait.TABLE, StaticArgumentTrait.ROW_SEMANTIC_TABLE));
        assertThat(arg.getPassThroughMode()).isEqualTo(PassThroughMode.KEY);
    }

    @Test
    void passColumnsThroughTraitMapsToAll() {
        final StaticArgument arg =
                StaticArgument.table(
                        "in",
                        Row.class,
                        false,
                        EnumSet.of(
                                StaticArgumentTrait.TABLE,
                                StaticArgumentTrait.SET_SEMANTIC_TABLE,
                                StaticArgumentTrait.PASS_COLUMNS_THROUGH));
        assertThat(arg.getPassThroughMode()).isEqualTo(PassThroughMode.ALL);
    }

    @Test
    void noPassThroughTraitMapsToNone() {
        final StaticArgument arg =
                StaticArgument.table(
                        "in",
                        Row.class,
                        false,
                        EnumSet.of(
                                StaticArgumentTrait.TABLE,
                                StaticArgumentTrait.SET_SEMANTIC_TABLE,
                                StaticArgumentTrait.NO_PASS_THROUGH));
        assertThat(arg.getPassThroughMode()).isEqualTo(PassThroughMode.NONE);
    }

    @Test
    void scalarArgIsKeyMode() {
        // Scalar args don't carry pass-through semantics; the derivation defaults to KEY,
        // which is harmless because consumers only consult the mode for table args.
        final StaticArgument arg = StaticArgument.scalar("s", DataTypes.INT(), false);
        assertThat(arg.getPassThroughMode()).isEqualTo(PassThroughMode.KEY);
    }

    @Test
    void conditionalNoPassThroughResolvesWhenConditionMatches() {
        final StaticArgument base =
                StaticArgument.table(
                                "in",
                                Row.class,
                                false,
                                EnumSet.of(
                                        StaticArgumentTrait.TABLE,
                                        StaticArgumentTrait.SET_SEMANTIC_TABLE))
                        .withConditionalTrait(StaticArgumentTrait.NO_PASS_THROUGH, ctx -> true);

        // Conditional traits are not active until applyConditionalTraits is invoked.
        assertThat(base.getPassThroughMode()).isEqualTo(PassThroughMode.KEY);

        final StaticArgument resolved = base.applyConditionalTraits(noopContext());
        assertThat(resolved.getPassThroughMode()).isEqualTo(PassThroughMode.NONE);
    }

    @Test
    void conditionalNoPassThroughStaysKeyWhenConditionDoesNotMatch() {
        final StaticArgument base =
                StaticArgument.table(
                                "in",
                                Row.class,
                                false,
                                EnumSet.of(
                                        StaticArgumentTrait.TABLE,
                                        StaticArgumentTrait.SET_SEMANTIC_TABLE))
                        .withConditionalTrait(StaticArgumentTrait.NO_PASS_THROUGH, ctx -> false);

        final StaticArgument resolved = base.applyConditionalTraits(noopContext());
        assertThat(resolved.getPassThroughMode()).isEqualTo(PassThroughMode.KEY);
    }

    @Test
    void declaringMutuallyExclusiveTraitsIsRejected() {
        assertThatThrownBy(
                        () ->
                                StaticArgument.table(
                                        "in",
                                        Row.class,
                                        false,
                                        EnumSet.of(
                                                StaticArgumentTrait.TABLE,
                                                StaticArgumentTrait.SET_SEMANTIC_TABLE,
                                                StaticArgumentTrait.PASS_COLUMNS_THROUGH,
                                                StaticArgumentTrait.NO_PASS_THROUGH)))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("mutually exclusive");
    }

    private static TraitContext noopContext() {
        return TraitContext.of(null, null, java.util.List.of());
    }
}
