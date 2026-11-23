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

package org.apache.flink.table.planner.functions.sql;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link BuiltInSqlFunction}. */
class BuiltInSqlFunctionTest {

    @Test
    void testVersioned() {
        final BuiltInSqlFunction function =
                BuiltInSqlFunction.newBuilder().name("OP").version(2).build();

        assertThat(function.getName()).isEqualTo("OP");
        assertThat(function.getQualifiedName()).isEqualTo("$OP$2");
        assertThat(function.isInternal()).isFalse();
        assertThat(function.getVersion()).contains(2);
    }

    @Test
    void testInternal() {
        final BuiltInSqlFunction internal =
                BuiltInSqlFunction.newBuilder().name("$OP$1").internal().build();

        assertThat(internal.getName()).isEqualTo("$OP$1");
        assertThat(internal.getQualifiedName()).isEqualTo("$OP$1");
        assertThat(internal.isInternal()).isTrue();
        assertThat(internal.getVersion()).isEmpty();
    }
}
