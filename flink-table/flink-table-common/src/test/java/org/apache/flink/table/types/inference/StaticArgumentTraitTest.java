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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StaticArgumentTrait}. */
class StaticArgumentTraitTest {

    @Test
    void noPassThroughIsIncompatibleWithPassColumnsThrough() {
        assertThat(StaticArgumentTrait.NO_PASS_THROUGH.getIncompatibleWith())
                .containsExactly(StaticArgumentTrait.PASS_COLUMNS_THROUGH);
    }

    @Test
    void passColumnsThroughIsIncompatibleWithNoPassThrough() {
        assertThat(StaticArgumentTrait.PASS_COLUMNS_THROUGH.getIncompatibleWith())
                .containsExactly(StaticArgumentTrait.NO_PASS_THROUGH);
    }

    @Test
    void semanticTraitsAreIncompatibleWithEachOther() {
        assertThat(StaticArgumentTrait.ROW_SEMANTIC_TABLE.getIncompatibleWith())
                .containsExactly(StaticArgumentTrait.SET_SEMANTIC_TABLE);
        assertThat(StaticArgumentTrait.SET_SEMANTIC_TABLE.getIncompatibleWith())
                .containsExactly(StaticArgumentTrait.ROW_SEMANTIC_TABLE);
    }

    /**
     * Symmetry: if X declares Y as incompatible, Y must declare X as incompatible. Easy to forget
     * when adding a new pair to the switch.
     */
    @Test
    void incompatibleWithIsSymmetric() {
        for (final StaticArgumentTrait left : StaticArgumentTrait.values()) {
            for (final StaticArgumentTrait right : left.getIncompatibleWith()) {
                assertThat(right.getIncompatibleWith())
                        .as("expected %s to declare %s as incompatible (symmetry)", right, left)
                        .contains(left);
            }
        }
    }
}
