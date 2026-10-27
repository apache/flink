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

package org.apache.flink.table.runtime.functions.ptf;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.dataview.ValueView;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.ExpressionEvaluator;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.StateTypeStrategy;
import org.apache.flink.table.types.inference.utils.CallContextMock;
import org.apache.flink.table.types.inference.utils.TableSemanticsMock;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.types.utils.DataTypeUtils;
import org.apache.flink.types.ColumnList;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the state that each specialization of {@link DeduplicateKeepLast} declares. */
class DeduplicateKeepLastTest {

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("user_id", DataTypes.STRING()),
                    DataTypes.FIELD("event", DataTypes.STRING()),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(3)));

    private static final DataType PAYLOAD_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("event", DataTypes.STRING()),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(3)));

    @Test
    void testArrivalOrderKeepsOnlyPayload() {
        final CallContextMock callContext = callContext(null, null, Duration.ofMinutes(5));
        final UserDefinedFunction function = specialize(callContext);

        assertThat(function).isInstanceOf(DeduplicateKeepLast.ArrivalOrder.class);
        final StateTypeStrategy current = currentState(function);
        assertThat(current.inferType(callContext)).contains(internalValueView(PAYLOAD_TYPE));
        assertThat(current.getTimeToLive(callContext)).contains(Duration.ofMinutes(5));

        final CallContextMock withoutTtl = callContext(null, null, null);
        assertThat(currentState(specialize(withoutTtl)).getTimeToLive(withoutTtl)).isEmpty();
    }

    @Test
    void testSequenceValuesKeepsOnlyPayload() {
        final CallContextMock callContext = callContext(List.of("ts"), null, null);
        final UserDefinedFunction function = specialize(callContext);

        assertThat(function).isInstanceOf(DeduplicateKeepLast.SequenceValues.class);
        assertThat(currentState(function).inferType(callContext))
                .contains(internalValueView(PAYLOAD_TYPE));
    }

    @Test
    void testEventTimeKeepsPayloadAndEventTime() {
        final CallContextMock callContext = callContext(null, "ts", Duration.ofMinutes(5));
        final UserDefinedFunction function = specialize(callContext);

        assertThat(function).isInstanceOf(DeduplicateKeepLast.EventTime.class);
        final StateTypeStrategy current = currentState(function);
        assertThat(current.inferType(callContext))
                .contains(
                        internalValueView(
                                DataTypes.ROW(
                                        DataTypes.FIELD(
                                                "payload",
                                                DataTypes.ROW(
                                                        DataTypes.FIELD(
                                                                "event", DataTypes.STRING()))),
                                        DataTypes.FIELD("event_time", DataTypes.BIGINT()))));
        assertThat(current.getTimeToLive(callContext)).contains(Duration.ofMinutes(5));
    }

    private static DataType internalValueView(final DataType type) {
        return ValueView.newValueViewDataType(DataTypeUtils.toInternalDataType(type));
    }

    private static StateTypeStrategy currentState(final UserDefinedFunction function) {
        final Map<String, StateTypeStrategy> state =
                function.getTypeInference(new DataTypeFactoryMock()).getStateTypeStrategies();
        assertThat(state).containsOnlyKeys("current");
        return state.get("current");
    }

    private static UserDefinedFunction specialize(final CallContext callContext) {
        return new DeduplicateKeepLast()
                .specialize(
                        new SpecializedContext() {
                            @Override
                            public CallContext getCallContext() {
                                return callContext;
                            }

                            @Override
                            public ReadableConfig getConfiguration() {
                                return new Configuration();
                            }

                            @Override
                            public ClassLoader getBuiltInClassLoader() {
                                return DeduplicateKeepLastTest.class.getClassLoader();
                            }

                            @Override
                            public ExpressionEvaluator createEvaluator(
                                    Expression expression,
                                    DataType outputDataType,
                                    DataTypes.Field... args) {
                                return context -> {
                                    throw new UnsupportedOperationException();
                                };
                            }

                            @Override
                            public ExpressionEvaluator createEvaluator(
                                    String sqlExpression,
                                    DataType outputDataType,
                                    DataTypes.Field... args) {
                                throw new UnsupportedOperationException();
                            }

                            @Override
                            public ExpressionEvaluator createEvaluator(
                                    BuiltInFunctionDefinition function,
                                    DataType outputDataType,
                                    DataType... args) {
                                throw new UnsupportedOperationException();
                            }
                        });
    }

    /** Declared arguments followed by the system arguments {@code on_time} and {@code uid}. */
    private static CallContextMock callContext(
            final List<String> sequenceBy, final String onTime, final Duration stateTtl) {
        final CallContextMock callContext = new CallContextMock();
        callContext.typeFactory = new DataTypeFactoryMock();
        callContext.functionDefinition = BuiltInFunctionDefinitions.DEDUPLICATE_KEEP_LAST;
        callContext.argumentDataTypes =
                List.of(
                        INPUT_TYPE,
                        DataTypes.DESCRIPTOR(),
                        DataTypes.INTERVAL(DataTypes.SECOND()),
                        DataTypes.BOOLEAN(),
                        DataTypes.DESCRIPTOR(),
                        DataTypes.STRING());
        callContext.argumentValues =
                Arrays.asList(
                        Optional.empty(),
                        Optional.ofNullable(sequenceBy).map(ColumnList::of),
                        Optional.ofNullable(stateTtl),
                        Optional.empty(),
                        Optional.ofNullable(onTime).map(name -> ColumnList.of(List.of(name))),
                        Optional.empty());
        callContext.outputDataType = Optional.of(DataTypes.ROW().notNull());
        callContext.tableSemantics =
                Map.of(
                        0,
                        new TableSemanticsMock(
                                INPUT_TYPE,
                                new int[] {0},
                                new int[0],
                                onTime == null ? -1 : 2,
                                null));
        return callContext;
    }
}
