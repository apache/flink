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

package org.apache.flink.table.functions.python;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.StaticArgument;
import org.apache.flink.table.types.inference.StaticArgumentTrait;
import org.apache.flink.table.types.inference.TypeInference;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PythonProcessTableFunction}. */
class PythonProcessTableFunctionTest {

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(DataTypes.FIELD("name", DataTypes.STRING()));
    private static final DataType STATE_TYPE =
            DataTypes.ROW(DataTypes.FIELD("count", DataTypes.BIGINT()));
    private static final DataType RESULT_TYPE =
            DataTypes.ROW(DataTypes.FIELD("result", DataTypes.STRING()));

    @Test
    void testCreatesOrderedTypeInference() {
        final PythonProcessTableFunction function = createFunction();

        final TypeInference typeInference = function.getTypeInference(null);
        final List<StaticArgument> arguments = typeInference.getStaticArguments().orElseThrow();

        assertThat(arguments).extracting(StaticArgument::getName).containsExactly("input", "limit");
        assertThat(arguments.get(0).is(StaticArgumentTrait.TABLE)).isTrue();
        assertThat(arguments.get(0).is(StaticArgumentTrait.SET_SEMANTIC_TABLE)).isTrue();
        assertThat(arguments.get(0).is(StaticArgumentTrait.REQUIRE_ON_TIME)).isFalse();
        assertThat(arguments.get(1).is(StaticArgumentTrait.SCALAR)).isTrue();
        assertThat(typeInference.getStateTypeStrategies()).containsOnlyKeys("memory");
        assertThat(function.getStateTimeToLive()).containsExactly(Duration.ofDays(1));
        assertThat(function.hasOnTimer()).isTrue();
        assertThat(function.getPythonEnv().getExecType()).isEqualTo(PythonEnv.ExecType.PROCESS);
    }

    @Test
    void testSupportsPolymorphicTableArgument() {
        final PythonProcessTableFunction function =
                new PythonProcessTableFunction(
                        "polymorphic",
                        new byte[] {1},
                        new String[] {"input"},
                        new DataType[] {null},
                        new boolean[] {true},
                        new String[] {"ROW_SEMANTIC_TABLE"},
                        new String[0],
                        new DataType[0],
                        new Duration[0],
                        RESULT_TYPE,
                        true,
                        false,
                        new PythonEnv(PythonEnv.ExecType.PROCESS));

        final StaticArgument argument =
                function.getTypeInference(null).getStaticArguments().orElseThrow().get(0);

        assertThat(argument.getName()).isEqualTo("input");
        assertThat(argument.is(StaticArgumentTrait.ROW_SEMANTIC_TABLE)).isTrue();
    }

    @Test
    void testRejectsInconsistentArgumentMetadata() {
        assertThatThrownBy(
                        () ->
                                new PythonProcessTableFunction(
                                        "invalid",
                                        new byte[] {1},
                                        new String[] {"input"},
                                        new DataType[0],
                                        new boolean[] {true},
                                        new String[] {"SET_SEMANTIC_TABLE"},
                                        new String[0],
                                        new DataType[0],
                                        new Duration[0],
                                        RESULT_TYPE,
                                        true,
                                        false,
                                        new PythonEnv(PythonEnv.ExecType.PROCESS)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Argument metadata must have equal lengths");
    }

    private static PythonProcessTableFunction createFunction() {
        return new PythonProcessTableFunction(
                "test_ptf",
                new byte[] {1, 2, 3},
                new String[] {"input", "limit"},
                new DataType[] {INPUT_TYPE, DataTypes.INT()},
                new boolean[] {true, false},
                new String[] {"SET_SEMANTIC_TABLE", ""},
                new String[] {"memory"},
                new DataType[] {STATE_TYPE},
                new Duration[] {Duration.ofDays(1)},
                RESULT_TYPE,
                true,
                true,
                new PythonEnv(PythonEnv.ExecType.PROCESS));
    }
}
