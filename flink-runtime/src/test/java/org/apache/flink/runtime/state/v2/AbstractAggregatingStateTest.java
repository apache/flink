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

package org.apache.flink.runtime.state.v2;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.MockStateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link AbstractAggregatingState}. */
class AbstractAggregatingStateTest extends AbstractKeyedStateTestBase {
    static class SumAggregator implements AggregateFunction<Integer, Integer, Integer> {
        private final int init;

        public SumAggregator(int init) {
            this.init = init;
        }

        @Override
        public Integer createAccumulator() {
            return init;
        }

        @Override
        public Integer add(Integer value, Integer accumulator) {
            return accumulator + value;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return a + b;
        }
    }

    @Test
    @SuppressWarnings({"unchecked"})
    public void testAggregating() {
        AggregateFunction<Integer, Integer, Integer> aggregator = new SumAggregator(1);
        AggregatingStateDescriptor<Integer, Integer, Integer> descriptor =
                new AggregatingStateDescriptor<>(
                        "testAggState", aggregator, BasicTypeInfo.INT_TYPE_INFO);
        AbstractAggregatingState<String, Void, Integer, Integer, Integer> state =
                new AbstractAggregatingState<>(aec, descriptor);

        aec.setCurrentContext(aec.buildContext("test", "test"));

        state.asyncClear();
        validateRequestRun(state, StateRequestType.CLEAR, null, 0);

        state.asyncGet();
        validateRequestRun(state, StateRequestType.AGGREGATING_GET, null, 0);

        state.asyncAdd(1);
        validateRequestRun(state, StateRequestType.AGGREGATING_GET, null, 1);
        validateRequestRun(state, StateRequestType.AGGREGATING_ADD, 2, 0);

        state.asyncAdd(5);
        validateRequestRun(state, StateRequestType.AGGREGATING_GET, null, 1);
        // the default value is 1
        validateRequestRun(state, StateRequestType.AGGREGATING_ADD, 6, 0);
    }

    @Test
    public void testMergeNamespace() throws Exception {
        AggregateFunction<Integer, Integer, Integer> aggregator = new SumAggregator(0);
        AggregatingStateDescriptor<Integer, Integer, Integer> descriptor =
                new AggregatingStateDescriptor<>(
                        "testState", aggregator, BasicTypeInfo.INT_TYPE_INFO);
        AsyncExecutionController<String> aec =
                new AsyncExecutionController<>(
                        new SyncMailboxExecutor(),
                        (a, b) -> {},
                        new AbstractAggregatingStateTest.AggregatingStateExecutor(),
                        1,
                        100,
                        10000,
                        1,
                        null);
        AbstractAggregatingState<String, String, Integer, Integer, Integer> aggregatingState =
                new AbstractAggregatingState<>(aec, descriptor);
        aec.setCurrentContext(aec.buildContext("test", "test"));
        aec.setCurrentNamespaceForState(aggregatingState, "1");
        aggregatingState.asyncAdd(1);
        aec.drainInflightRecords(0);
        assertThat(AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.size())
                .isEqualTo(1);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "1")))
                .isEqualTo(1);
        aec.setCurrentNamespaceForState(aggregatingState, "2");
        aggregatingState.asyncAdd(2);
        aec.drainInflightRecords(0);
        assertThat(AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.size())
                .isEqualTo(2);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "1")))
                .isEqualTo(1);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "2")))
                .isEqualTo(2);
        aec.setCurrentNamespaceForState(aggregatingState, "3");
        aggregatingState.asyncAdd(3);
        aec.drainInflightRecords(0);
        assertThat(AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.size())
                .isEqualTo(3);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "1")))
                .isEqualTo(1);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "2")))
                .isEqualTo(2);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "3")))
                .isEqualTo(3);

        List<String> sources = new ArrayList<>(Arrays.asList("1", "2", "3"));
        aggregatingState.asyncMergeNamespaces("0", sources);
        aec.drainInflightRecords(0);
        assertThat(AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.size())
                .isEqualTo(1);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "0")))
                .isEqualTo(6);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "1")))
                .isNull();
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "2")))
                .isNull();
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "3")))
                .isNull();

        aec.setCurrentNamespaceForState(aggregatingState, "4");
        aggregatingState.asyncAdd(4);
        aec.drainInflightRecords(0);
        assertThat(AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.size())
                .isEqualTo(2);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "0")))
                .isEqualTo(6);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "4")))
                .isEqualTo(4);

        List<String> sources1 = new ArrayList<>(Arrays.asList("4"));
        aggregatingState.asyncMergeNamespaces("0", sources1);
        aec.drainInflightRecords(0);

        assertThat(AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.size())
                .isEqualTo(1);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "0")))
                .isEqualTo(10);
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "1")))
                .isNull();
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "2")))
                .isNull();
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "3")))
                .isNull();
        assertThat(
                        AbstractAggregatingStateTest.AggregatingStateExecutor.hashMap.get(
                                Tuple2.of("test", "4")))
                .isNull();
    }

    static class AggregatingStateExecutor implements StateExecutor {

        private static final HashMap<Tuple2<String, String>, Integer> hashMap = new HashMap<>();

        @SuppressWarnings({"unchecked", "rawtypes"})
        @Override
        public CompletableFuture<Void> executeBatchRequests(
                StateRequestContainer stateRequestContainer) {
            for (StateRequest stateRequest :
                    ((MockStateRequestContainer) stateRequestContainer).getStateRequestList()) {
                String key = (String) stateRequest.getRecordContext().getKey();
                String namespace = (String) stateRequest.getNamespace();
                if (stateRequest.getRequestType() == StateRequestType.AGGREGATING_ADD) {
                    if (stateRequest.getPayload() == null) {
                        hashMap.remove(Tuple2.of(key, namespace));
                        stateRequest.getFuture().complete(null);
                    } else {
                        hashMap.put(Tuple2.of(key, namespace), (Integer) stateRequest.getPayload());
                        stateRequest.getFuture().complete(null);
                    }
                } else if (stateRequest.getRequestType() == StateRequestType.AGGREGATING_GET) {
                    Integer val = hashMap.get(Tuple2.of(key, namespace));
                    stateRequest.getFuture().complete(val);
                } else {
                    throw new UnsupportedOperationException("Unsupported type");
                }
            }
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return future;
        }

        @Override
        public StateRequestContainer createStateRequestContainer() {
            return new MockStateRequestContainer();
        }

        @Override
        public boolean fullyLoaded() {
            return false;
        }

        @Override
        public void shutdown() {}
    }
}
