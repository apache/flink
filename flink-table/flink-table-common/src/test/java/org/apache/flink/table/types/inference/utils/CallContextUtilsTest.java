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

package org.apache.flink.table.types.inference.utils;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.ChangelogMode;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link CallContextUtils}. */
class CallContextUtilsTest {

    private enum Mode {
        FIRST,
        LAST
    }

    @Test
    void testTableSemanticsOrThrow() {
        final TableSemanticsMock semantics =
                new TableSemanticsMock(
                        DataTypes.ROW(DataTypes.FIELD("id", DataTypes.INT())),
                        new int[0],
                        new int[0],
                        -1,
                        ChangelogMode.insertOnly());
        final CallContextMock callContext = new CallContextMock();
        callContext.name = "F";
        callContext.tableSemantics = Map.of(0, semantics);

        assertThat(CallContextUtils.getTableSemanticsOrThrow(callContext, 0)).isSameAs(semantics);
        assertThatThrownBy(() -> CallContextUtils.getTableSemanticsOrThrow(callContext, 1))
                .isInstanceOf(ValidationException.class)
                .hasMessage("The argument at position 1 of function 'F' must be a table argument.");
    }

    @Test
    void testEnumArgument() {
        assertThat(CallContextUtils.getEnumArgument(literal("last"), 0, Mode.class))
                .contains(Mode.LAST);
        assertThat(CallContextUtils.getEnumArgument(literal("LAST"), 0, Mode.class)).isEmpty();
        assertThat(CallContextUtils.getEnumArgument(nullLiteral(), 0, Mode.class)).isEmpty();
    }

    @Test
    void testEnumArgumentValidation() {
        assertThat(validateMode(literal("first"))).isEmpty();
        assertThat(validateMode(nullLiteral())).isEmpty();
        assertThat(validateMode(literal("FIRST")))
                .contains(
                        "F requires mode => 'first' or mode => 'last', but received "
                                + "mode => 'FIRST'.");
        assertThat(validateMode(nonLiteral()))
                .contains(
                        "F requires mode => 'first' or mode => 'last', but received a "
                                + "non-literal expression.");
    }

    @Test
    void testStateTtlValidation() {
        assertThat(CallContextUtils.validateStateTtl(literal(Duration.ofHours(1)), 0)).isEmpty();
        assertThat(CallContextUtils.validateStateTtl(nullLiteral(), 0)).isEmpty();
        assertThat(CallContextUtils.validateStateTtl(literal(Duration.ofSeconds(-1)), 0))
                .contains(
                        "The 'state_ttl' argument must not be negative, but was '-1 s'. Use "
                                + "INTERVAL '0' SECOND to retain state indefinitely.");
        assertThat(CallContextUtils.validateStateTtl(nonLiteral(), 0))
                .contains(
                        "The 'state_ttl' argument must be a constant INTERVAL literal, but "
                                + "received a non-literal expression. Use a literal such as "
                                + "INTERVAL '1' HOUR.");
    }

    @Test
    void testStateTtl() {
        assertThat(CallContextUtils.getStateTtl(literal(Duration.ofHours(1)), 0))
                .contains(Duration.ofHours(1));
        assertThat(CallContextUtils.getStateTtl(nullLiteral(), 0)).isEmpty();
    }

    private static Optional<String> validateMode(final CallContextMock callContext) {
        return CallContextUtils.validateEnumArgument(callContext, 0, "mode", "F", Mode.class);
    }

    private static CallContextMock literal(final Object value) {
        return callContext(true, false, Optional.of(value));
    }

    private static CallContextMock nullLiteral() {
        return callContext(true, true, Optional.empty());
    }

    private static CallContextMock nonLiteral() {
        return callContext(false, false, Optional.empty());
    }

    private static CallContextMock callContext(
            final boolean isLiteral, final boolean isNull, final Optional<?> value) {
        final CallContextMock callContext = new CallContextMock();
        callContext.argumentLiterals = List.of(isLiteral);
        callContext.argumentNulls = List.of(isNull);
        callContext.argumentValues = List.of(value);
        return callContext;
    }
}
