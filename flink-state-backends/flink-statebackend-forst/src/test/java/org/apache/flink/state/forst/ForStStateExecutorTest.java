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

package org.apache.flink.state.forst;

import org.apache.flink.api.common.state.v2.StateIterator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.v2.AbstractKeyedState;

import org.forstdb.WriteOptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link ForStStateExecutor}. */
class ForStStateExecutorTest extends ForStDBOperationTestBase {

    @Test
    @SuppressWarnings("unchecked")
    void testExecuteValueStateRequest() throws Exception {
        ForStStateExecutor forStStateExecutor =
                new ForStStateExecutor(false, false, 3, 1, db, new WriteOptions());
        ForStValueState<Integer, VoidNamespace, String> state1 =
                buildForStValueState("value-state-1");
        ForStValueState<Integer, VoidNamespace, String> state2 =
                buildForStValueState("value-state-2");

        StateRequestContainer stateRequestContainer =
                forStStateExecutor.createStateRequestContainer();
        assertTrue(stateRequestContainer.isEmpty());

        // 1. Update value state: keyRange [0, keyNum)
        int keyNum = 1000;
        for (int i = 0; i < keyNum; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, "test-" + i, i * 2));
        }

        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        List<StateRequest<?, ?, ?, ?>> checkList = new ArrayList<>();
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        // 2. Get value state: keyRange [0, keyNum)
        //    Update value state: keyRange [keyNum, keyNum + 100]
        for (int i = 0; i < keyNum; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            StateRequest<?, ?, ?, ?> getRequest =
                    buildStateRequest(state, StateRequestType.VALUE_GET, i, null, i * 2);
            stateRequestContainer.offer(getRequest);
            checkList.add(getRequest);
        }
        for (int i = keyNum; i < keyNum + 100; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, "test-" + i, i * 2));
        }
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        // 3. Check value state Get result : [0, keyNum)
        for (StateRequest<?, ?, ?, ?> getRequest : checkList) {
            assertThat(getRequest.getRequestType()).isEqualTo(StateRequestType.VALUE_GET);
            int key = (Integer) getRequest.getRecordContext().getKey();
            assertThat(getRequest.getRecordContext().getRecord()).isEqualTo(key * 2);
            assertThat(((TestStateFuture<String>) getRequest.getFuture()).getCompletedResult())
                    .isEqualTo("test-" + key);
        }

        // 4. Clear value state:  keyRange [keyNum - 100, keyNum)
        //    Update state with null-value : keyRange [keyNum, keyNum + 100]
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        for (int i = keyNum - 100; i < keyNum; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.CLEAR, i, null, i * 2));
        }
        for (int i = keyNum; i < keyNum + 100; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, null, i * 2));
        }
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        // 5. Check that the deleted value is null :  keyRange [keyNum - 100, keyNum + 100)
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        checkList.clear();
        for (int i = keyNum - 100; i < keyNum + 100; i++) {
            ForStValueState<Integer, VoidNamespace, String> state = (i % 2 == 0 ? state1 : state2);
            StateRequest<?, ?, ?, ?> getRequest =
                    buildStateRequest(state, StateRequestType.VALUE_GET, i, null, i * 2);
            stateRequestContainer.offer(getRequest);
            checkList.add(getRequest);
        }
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();
        for (StateRequest<?, ?, ?, ?> getRequest : checkList) {
            assertThat(getRequest.getRequestType()).isEqualTo(StateRequestType.VALUE_GET);
            assertThat(((TestStateFuture<String>) getRequest.getFuture()).getCompletedResult())
                    .isEqualTo(null);
        }
        forStStateExecutor.shutdown();
    }

    @Test
    void testExecuteMapStateRequest() throws Exception {
        ForStStateExecutor forStStateExecutor =
                new ForStStateExecutor(true, false, 3, 1, db, new WriteOptions());
        ForStMapState<Integer, VoidNamespace, String, String> state =
                buildForStMapState("map-state");
        StateRequestContainer stateRequestContainer =
                forStStateExecutor.createStateRequestContainer();
        assertTrue(stateRequestContainer.isEmpty());

        // 1. prepare put data: keyRange [0, 100)
        for (int i = 0; i < 100; i++) {
            stateRequestContainer.offer(
                    buildMapRequest(
                            state,
                            StateRequestType.MAP_PUT,
                            i,
                            Tuple2.of("uk-" + i, "uv-" + i),
                            i * 2));
        }
        for (int i = 50; i < 100; i++) {
            HashMap<String, String> map = new HashMap<>(100);
            for (int j = 0; j < 50; j++) {
                map.put("ukk-" + j, "uvv-" + j);
            }
            stateRequestContainer.offer(
                    buildMapRequest(state, StateRequestType.MAP_PUT_ALL, i, map, i * 2));
        }
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        List<StateRequest<?, ?, ?, ?>> checkList = new ArrayList<>();

        // 2. check the number of user key under primary key is correct
        for (int i = 0; i < 100; i++) {
            StateRequest<?, ?, ?, ?> iterRequest =
                    buildStateRequest(state, StateRequestType.MAP_ITER_KEY, i, null, i * 2);
            stateRequestContainer.offer(iterRequest);
            checkList.add(iterRequest);
        }

        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        for (int i = 0; i < 50; i++) { // 1 user key per primary key
            StateIterator<String> iter =
                    (StateIterator<String>)
                            ((TestStateFuture) checkList.get(i).getFuture()).getCompletedResult();
            AtomicInteger count = new AtomicInteger(0);
            iter.onNext(
                    k -> {
                        count.incrementAndGet();
                    });
            assertThat(count.get()).isEqualTo(1);
            assertThat(iter.isEmpty()).isFalse();
        }

        for (int i = 50; i < 100; i++) { // 51 user keys per primary key
            StateIterator<String> iter =
                    (StateIterator<String>)
                            ((TestStateFuture) checkList.get(i).getFuture()).getCompletedResult();
            AtomicInteger count = new AtomicInteger(0);
            iter.onNext(
                    k -> {
                        count.incrementAndGet();
                    });
            assertThat(count.get()).isEqualTo(51);
            assertThat(iter.isEmpty()).isFalse();
        }

        stateRequestContainer = forStStateExecutor.createStateRequestContainer();

        // 3. delete primary key [75,100)
        for (int i = 75; i < 100; i++) {
            stateRequestContainer.offer(
                    buildMapRequest(state, StateRequestType.CLEAR, i, null, i * 2));
        }

        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        checkList.clear();
        // 4. check primary key [75,100) is deleted
        for (int i = 0; i < 100; i++) {
            StateRequest<?, ?, ?, ?> iterRequest =
                    buildStateRequest(state, StateRequestType.MAP_IS_EMPTY, i, null, i * 2);
            stateRequestContainer.offer(iterRequest);
            checkList.add(iterRequest);
        }
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();
        for (int i = 0; i < 75; i++) { // not empty
            boolean empty =
                    (Boolean) ((TestStateFuture) checkList.get(i).getFuture()).getCompletedResult();
            assertThat(empty).isFalse();
        }
        for (int i = 75; i < 100; i++) { // empty
            boolean empty =
                    (Boolean) ((TestStateFuture) checkList.get(i).getFuture()).getCompletedResult();
            assertThat(empty).isTrue();
        }

        forStStateExecutor.shutdown();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testExecuteAggregatingStateRequest() throws Exception {
        ForStStateExecutor forStStateExecutor =
                new ForStStateExecutor(false, false, 4, 1, db, new WriteOptions());
        ForStAggregatingState<String, ?, Integer, Integer, Integer> state =
                buildForStSumAggregateState("agg-state-1");

        StateRequestContainer stateRequestContainer =
                forStStateExecutor.createStateRequestContainer();
        assertTrue(stateRequestContainer.isEmpty());

        // 1. init aggregateValue for every 1000 key
        int keyNum = 1000;
        for (int i = 0; i < keyNum; i++) {
            stateRequestContainer.offer(
                    buildStateRequest(
                            state, StateRequestType.AGGREGATING_ADD, "" + i, i, "record" + i));
        }

        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        // 2. get all value and verify
        List<StateRequest<String, ?, Integer, Integer>> requests = new ArrayList<>();
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        for (int i = 0; i < keyNum; i++) {
            StateRequest<String, ?, Integer, Integer> r =
                    (StateRequest<String, ?, Integer, Integer>)
                            buildStateRequest(
                                    state,
                                    StateRequestType.AGGREGATING_GET,
                                    "" + i,
                                    null,
                                    "record" + i);
            requests.add(r);
            stateRequestContainer.offer(r);
        }

        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        for (StateRequest<String, ?, Integer, Integer> request : requests) {
            assertThat(request.getRequestType()).isEqualTo(StateRequestType.AGGREGATING_GET);
            String key = request.getRecordContext().getKey();
            assertThat(request.getRecordContext().getRecord()).isEqualTo("record" + key);
            assertThat(((TestStateFuture<Integer>) request.getFuture()).getCompletedResult())
                    .isEqualTo(Integer.parseInt(key));
        }

        // 3. add more value for the aggregate state
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        int addCnt = 10;
        for (int i = 0; i < keyNum; i++) {
            for (int j = 0; j < addCnt; j++) {
                StateRequest<String, ?, Integer, Integer> r =
                        (StateRequest<String, ?, Integer, Integer>)
                                buildStateRequest(
                                        state, StateRequestType.AGGREGATING_ADD, "" + i, 1, i * 2);
                requests.add(r);
                stateRequestContainer.offer(r);
            }
        }

        // clear first 100 state
        for (int i = 0; i < 100; i++) {
            StateRequest<String, ?, Integer, Integer> r =
                    (StateRequest<String, ?, Integer, Integer>)
                            buildStateRequest(
                                    state, StateRequestType.CLEAR, "" + i, null, "record" + i);
            requests.add(r);
            stateRequestContainer.offer(r);
        }

        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        // 4. read and verify the updated aggregate state
        requests = new ArrayList<>();
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        for (int i = 0; i < keyNum; i++) {
            StateRequest<String, ?, Integer, Integer> r =
                    (StateRequest<String, ?, Integer, Integer>)
                            buildStateRequest(
                                    state,
                                    StateRequestType.AGGREGATING_GET,
                                    "" + i,
                                    null,
                                    "record" + i);
            requests.add(r);
            stateRequestContainer.offer(r);
        }

        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        for (int i = 0; i < 100; i++) {
            StateRequest<String, ?, Integer, Integer> request = requests.get(i);
            assertThat(request.getRequestType()).isEqualTo(StateRequestType.AGGREGATING_GET);
            assertThat(((TestStateFuture<Integer>) request.getFuture()).getCompletedResult())
                    .isEqualTo(null);
        }
        for (int i = 100; i < keyNum; i++) {
            StateRequest<String, ?, Integer, Integer> request = requests.get(i);
            assertThat(request.getRequestType()).isEqualTo(StateRequestType.AGGREGATING_GET);
            String key = request.getRecordContext().getKey();
            assertThat(request.getRecordContext().getRecord()).isEqualTo("record" + key);
            assertThat(((TestStateFuture<Integer>) request.getFuture()).getCompletedResult())
                    .isEqualTo(1);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <K, N, V, R> StateRequest<?, ?, ?, ?> buildStateRequest(
            AbstractKeyedState<K, N, V> innerTable,
            StateRequestType requestType,
            K key,
            V value,
            R record) {
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 128);
        RecordContext<K> recordContext =
                new RecordContext<>(record, key, t -> {}, keyGroup, new Epoch(0));
        TestStateFuture stateFuture = new TestStateFuture<>();
        return new StateRequest<>(innerTable, requestType, value, stateFuture, recordContext);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <K, N, UK, UV, R> StateRequest<?, ?, ?, ?> buildMapRequest(
            ForStMapState<K, N, UK, UV> innerTable,
            StateRequestType requestType,
            K key,
            Object value,
            R record) {
        int keyGroup = KeyGroupRangeAssignment.assignToKeyGroup(key, 128);
        RecordContext<K> recordContext =
                new RecordContext<>(record, key, t -> {}, keyGroup, new Epoch(0));
        TestStateFuture stateFuture = new TestStateFuture<>();
        return new StateRequest<>(innerTable, requestType, value, stateFuture, recordContext);
    }
}
