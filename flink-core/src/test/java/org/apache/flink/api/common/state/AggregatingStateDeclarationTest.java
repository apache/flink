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

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link AggregatingStateDeclaration}. */
class AggregatingStateDeclarationTest {

    private AggregatingStateDeclaration<Integer, Integer, Integer> aggregatingStateDeclaration;
    private AggregateFunction<Integer, Integer, Integer> aggregateFunction;

    @BeforeEach
    void setUp() {
        aggregateFunction =
                new AggregateFunction<Integer, Integer, Integer>() {
                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(Integer value, Integer accumulator) {
                        return 0;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return 0;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return 0;
                    }
                };
        aggregatingStateDeclaration =
                StateDeclarations.aggregatingStateBuilder(
                                "aggregatingState", TypeDescriptors.INT, aggregateFunction)
                        .build();
    }

    @Test
    void testAggregatingStateDeclarationName() {
        assertThat(aggregatingStateDeclaration.getName()).isEqualTo("aggregatingState");
    }

    @Test
    void testAggregatingStateDeclarationFunc() {
        assertThat(aggregatingStateDeclaration.getAggregateFunction()).isEqualTo(aggregateFunction);
    }

    @Test
    void testAggregatingStateDeclarationType() {
        assertThat(aggregatingStateDeclaration.getTypeDescriptor()).isEqualTo(TypeDescriptors.INT);
    }

    @Test
    void testAggregatingStateDeclarationDist() {
        assertThat(aggregatingStateDeclaration.getRedistributionMode())
                .isEqualTo(StateDeclaration.RedistributionMode.NONE);
    }
}
