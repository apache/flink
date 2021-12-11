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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;

import org.junit.jupiter.api.Test;

import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.assertj.core.api.Assertions.assertThat;

class CastRuleProviderTest {

    private static final LogicalType DISTINCT_INT =
            DistinctType.newBuilder(ObjectIdentifier.of("a", "b", "c"), INT().getLogicalType())
                    .build();
    private static final LogicalType DISTINCT_BIG_INT =
            DistinctType.newBuilder(ObjectIdentifier.of("a", "b", "c"), BIGINT().getLogicalType())
                    .build();
    private static final LogicalType INT = INT().getLogicalType();

    @Test
    void testResolveDistinctTypeToIdentityCastRule() {
        assertThat(CastRuleProvider.resolve(DISTINCT_INT, INT)).isSameAs(IdentityCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(INT, DISTINCT_INT)).isSameAs(IdentityCastRule.INSTANCE);
        assertThat(CastRuleProvider.resolve(DISTINCT_INT, DISTINCT_INT))
                .isSameAs(IdentityCastRule.INSTANCE);
    }

    @Test
    void testResolveIntToBigIntWithDistinct() {
        assertThat(CastRuleProvider.resolve(INT, DISTINCT_BIG_INT))
                .isSameAs(NumericPrimitiveCastRule.INSTANCE);
    }

    @Test
    void testResolveArrayIntToBigIntWithDistinct() {
        assertThat(CastRuleProvider.resolve(new ArrayType(INT), new ArrayType(DISTINCT_BIG_INT)))
                .isSameAs(ArrayToArrayCastRule.INSTANCE);
    }
}
