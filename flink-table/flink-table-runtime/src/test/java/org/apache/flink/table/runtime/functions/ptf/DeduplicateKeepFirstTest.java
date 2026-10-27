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

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the state that each specialization of {@link DeduplicateKeepFirst} declares. */
class DeduplicateKeepFirstTest {

    private static final DataType INPUT_TYPE =
            DataTypes.ROW(
                    DataTypes.FIELD("user_name", DataTypes.STRING()),
                    DataTypes.FIELD("action", DataTypes.STRING()),
                    DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(3)));

    private static final DataType OUTPUT_TYPE =
            DataTypes.ROW(
                            DataTypes.FIELD("user_name", DataTypes.STRING()),
                            DataTypes.FIELD("action", DataTypes.STRING()),
                            DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(3)))
                    .notNull();

    @Test
    void testProcessingTimeDeclaresOnlySeenState() {
        final CallContextMock callContext = callContext(-1, Duration.ofSeconds(5));
        final UserDefinedFunction function = specialize(callContext);

        assertThat(function).isInstanceOf(DeduplicateKeepFirst.ProcessingTime.class);
        final Map<String, StateTypeStrategy> state = stateStrategies(function);
        assertThat(state).containsOnlyKeys("seen");
        assertThat(state.get("seen").inferType(callContext))
                .contains(ValueView.newValueViewDataType(DataTypes.BOOLEAN().notNull()));
        assertThat(state.get("seen").getTimeToLive(callContext)).contains(Duration.ofSeconds(5));

        final CallContextMock withoutTtl = callContext(-1, null);
        assertThat(stateStrategies(specialize(withoutTtl)).get("seen").getTimeToLive(withoutTtl))
                .isEmpty();
    }

    @Test
    void testEventTimeDeclaresSeenAndCandidateState() {
        final CallContextMock callContext = callContext(2, Duration.ofSeconds(5));
        final UserDefinedFunction function = specialize(callContext);

        assertThat(function).isInstanceOf(DeduplicateKeepFirst.EventTime.class);
        final Map<String, StateTypeStrategy> state = stateStrategies(function);
        assertThat(state.keySet()).containsExactly("seen", "candidate");
        assertThat(state.get("seen").getTimeToLive(callContext)).contains(Duration.ofSeconds(5));

        final DataType candidate =
                DataTypes.ROW(
                        DataTypes.FIELD(
                                "payload",
                                DataTypes.ROW(
                                        DataTypes.FIELD("action", DataTypes.STRING()),
                                        DataTypes.FIELD("ts", DataTypes.TIMESTAMP_LTZ(3)))),
                        DataTypes.FIELD("event_time", DataTypes.BIGINT()));
        assertThat(state.get("candidate").inferType(callContext))
                .contains(
                        ValueView.newValueViewDataType(
                                DataTypeUtils.toInternalDataType(candidate)));
        // The timer, not retention, clears the candidate, so state_ttl must not apply to it.
        assertThat(state.get("candidate").getTimeToLive(callContext)).contains(Duration.ZERO);
    }

    private static Map<String, StateTypeStrategy> stateStrategies(
            final UserDefinedFunction function) {
        return function.getTypeInference(new DataTypeFactoryMock()).getStateTypeStrategies();
    }

    private static UserDefinedFunction specialize(final CallContext callContext) {
        return new DeduplicateKeepFirst()
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
                                return DeduplicateKeepFirstTest.class.getClassLoader();
                            }

                            @Override
                            public ExpressionEvaluator createEvaluator(
                                    Expression expression,
                                    DataType outputDataType,
                                    DataTypes.Field... args) {
                                throw new UnsupportedOperationException();
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

    private static CallContextMock callContext(final int timeColumn, final Duration stateTtl) {
        final CallContextMock callContext = new CallContextMock();
        callContext.typeFactory = new DataTypeFactoryMock();
        callContext.functionDefinition = BuiltInFunctionDefinitions.DEDUPLICATE_KEEP_FIRST;
        callContext.argumentDataTypes =
                List.of(INPUT_TYPE, DataTypes.INTERVAL(DataTypes.SECOND()), DataTypes.BOOLEAN());
        callContext.argumentValues =
                Arrays.asList(Optional.empty(), Optional.ofNullable(stateTtl), Optional.empty());
        callContext.outputDataType = Optional.of(OUTPUT_TYPE);
        callContext.tableSemantics =
                Map.of(
                        0,
                        new TableSemanticsMock(
                                INPUT_TYPE, new int[] {0}, new int[0], timeColumn, null));
        return callContext;
    }
}
