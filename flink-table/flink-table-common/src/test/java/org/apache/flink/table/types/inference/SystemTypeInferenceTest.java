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
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.utils.CallContextMock;
import org.apache.flink.types.ColumnList;

import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SystemTypeInference}. */
class SystemTypeInferenceTest {

    @Test
    void testOnTimeRejectedIfDisabled() {
        assertThatThrownBy(() -> infer(true, ColumnList.of("ts"), null))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "The 'on_time' argument is not supported because function 'f' "
                                + "does not support time attributes.");
    }

    @Test
    void testUidAndEmptyOnTimeAllowedIfOnTimeDisabled() {
        assertThat(infer(true, ColumnList.of(), "my_uid")).isPresent();
    }

    @Test
    void testOnTimeAllowedByDefault() {
        assertThat(infer(false, ColumnList.of("ts"), null)).isPresent();
    }

    private static Optional<List<DataType>> infer(
            boolean disableOnTime, ColumnList onTime, @Nullable String uid) {
        final BuiltInFunctionDefinition.Builder builder =
                BuiltInFunctionDefinition.newBuilder()
                        .name("f")
                        .kind(FunctionKind.PROCESS_TABLE)
                        .staticArguments(StaticArgument.scalar("i", DataTypes.INT(), false))
                        .outputTypeStrategy(TypeStrategies.explicit(DataTypes.INT()));
        final BuiltInFunctionDefinition definition =
                builder.disableOnTimeArgument(disableOnTime).build();

        final TypeInference inference =
                SystemTypeInference.of(definition, definition.getTypeInference(null));

        final CallContextMock callContext = new CallContextMock();
        callContext.name = "f";
        callContext.functionDefinition = definition;
        callContext.argumentDataTypes =
                List.of(DataTypes.INT(), DataTypes.DESCRIPTOR(), DataTypes.STRING());
        callContext.argumentValues =
                List.of(Optional.empty(), Optional.of(onTime), Optional.ofNullable(uid));
        callContext.argumentNulls = List.of(false, false, uid == null);
        callContext.argumentLiterals = List.of(false, true, uid != null);

        return inference.getInputTypeStrategy().inferInputTypes(callContext, true);
    }
}
