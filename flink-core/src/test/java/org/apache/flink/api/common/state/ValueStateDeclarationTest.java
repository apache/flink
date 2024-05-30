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

import org.apache.flink.api.common.typeinfo.TypeDescriptor;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.types.BooleanValue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ValueStateDeclaration}. */
class ValueStateDeclarationTest {

    private static TypeDescriptor<BooleanValue> booleanValueDescriptor;
    private static ValueStateDeclaration<BooleanValue> valueStateDeclaration;

    @BeforeAll
    static void setUp() throws ReflectiveOperationException {
        booleanValueDescriptor =
                TypeDescriptors.value((TypeDescriptor<BooleanValue>) () -> BooleanValue.class);
        valueStateDeclaration = StateDeclarations.valueState("valueState", booleanValueDescriptor);
    }

    @Test
    void testValueStateDeclarationName() {
        assertThat(valueStateDeclaration.getName()).isEqualTo("valueState");
    }

    @Test
    void testValueStateDeclarationDistribution() {
        assertThat(valueStateDeclaration.getRedistributionMode())
                .isEqualTo(StateDeclaration.RedistributionMode.NONE);
    }

    @Test
    void testValueStateDeclarationType() {
        assertThat(valueStateDeclaration.getTypeDescriptor()).isEqualTo(booleanValueDescriptor);
    }
}
