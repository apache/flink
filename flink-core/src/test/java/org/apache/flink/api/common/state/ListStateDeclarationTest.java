/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.state;

import org.apache.flink.api.common.typeinfo.TypeDescriptors;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ListStateDeclaration}. */
class ListStateDeclarationTest {

    @Test
    void testListStateDeclarationName() {
        ListStateDeclaration<Integer> listStateDeclaration =
                StateDeclarations.listStateBuilder("listState", TypeDescriptors.INT).build();
        assertThat(listStateDeclaration.getName()).isEqualTo("listState");
    }

    @Test
    void testListStateDeclarationDistribution() {
        ListStateDeclaration<Integer> listStateDefault =
                StateDeclarations.listStateBuilder("listState", TypeDescriptors.INT).build();
        assertThat(listStateDefault.getRedistributionStrategy())
                .isEqualTo(ListStateDeclaration.RedistributionStrategy.SPLIT);
        assertThat(listStateDefault.getRedistributionMode())
                .isEqualTo(StateDeclaration.RedistributionMode.NONE);

        ListStateDeclaration<Integer> listStateCustomized =
                StateDeclarations.listStateBuilder("listState", TypeDescriptors.INT)
                        .redistributeBy(ListStateDeclaration.RedistributionStrategy.UNION)
                        .redistributeWithMode(StateDeclaration.RedistributionMode.REDISTRIBUTABLE)
                        .build();
        assertThat(listStateCustomized.getRedistributionStrategy())
                .isEqualTo(ListStateDeclaration.RedistributionStrategy.UNION);
        assertThat(listStateCustomized.getRedistributionMode())
                .isEqualTo(StateDeclaration.RedistributionMode.REDISTRIBUTABLE);
    }

    @Test
    void testListStateDeclarationType() {
        ListStateDeclaration<Integer> listStateDeclaration =
                StateDeclarations.listStateBuilder("listState", TypeDescriptors.INT).build();
        assertThat(listStateDeclaration.getTypeDescriptor()).isEqualTo(TypeDescriptors.INT);
    }
}
