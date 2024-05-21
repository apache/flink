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

import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.RecordContext;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.v2.InternalKeyedState;

import org.junit.jupiter.api.Test;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link ForStStateExecutor}. */
public class ForStStateExecutorTest extends ForStDBOperationTestBase {

    @Test
    @SuppressWarnings("unchecked")
    public void testExecuteValueStateRequest() throws Exception {
        ForStStateExecutor forStStateExecutor = new ForStStateExecutor(4, db, new WriteOptions());
        ForStValueState<Integer, String> state1 = buildForStValueState("value-state-1");
        ForStValueState<Integer, String> state2 = buildForStValueState("value-state-2");

        StateRequestContainer stateRequestContainer =
                forStStateExecutor.createStateRequestContainer();
        assertTrue(stateRequestContainer.isEmpty());

        // 1. Update value state: keyRange [0, keyNum)
        int keyNum = 1000;
        for (int i = 0; i < keyNum; i++) {
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, "test-" + i, i * 2));
        }

        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        List<StateRequest<?, ?, ?>> checkList = new ArrayList<>();
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        // 2. Get value state: keyRange [0, keyNum)
        //    Update value state: keyRange [keyNum, keyNum + 100]
        for (int i = 0; i < keyNum; i++) {
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            StateRequest<?, ?, ?> getRequest =
                    buildStateRequest(state, StateRequestType.VALUE_GET, i, null, i * 2);
            stateRequestContainer.offer(getRequest);
            checkList.add(getRequest);
        }
        for (int i = keyNum; i < keyNum + 100; i++) {
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, "test-" + i, i * 2));
        }
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        // 3. Check value state Get result : [0, keyNum)
        for (StateRequest<?, ?, ?> getRequest : checkList) {
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
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.CLEAR, i, null, i * 2));
        }
        for (int i = keyNum; i < keyNum + 100; i++) {
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            stateRequestContainer.offer(
                    buildStateRequest(state, StateRequestType.VALUE_UPDATE, i, null, i * 2));
        }
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();

        // 5. Check that the deleted value is null :  keyRange [keyNum - 100, keyNum + 100)
        stateRequestContainer = forStStateExecutor.createStateRequestContainer();
        checkList.clear();
        for (int i = keyNum - 100; i < keyNum + 100; i++) {
            ForStValueState<Integer, String> state = (i % 2 == 0 ? state1 : state2);
            StateRequest<?, ?, ?> getRequest =
                    buildStateRequest(state, StateRequestType.VALUE_GET, i, null, i * 2);
            stateRequestContainer.offer(getRequest);
            checkList.add(getRequest);
        }
        forStStateExecutor.executeBatchRequests(stateRequestContainer).get();
        for (StateRequest<?, ?, ?> getRequest : checkList) {
            assertThat(getRequest.getRequestType()).isEqualTo(StateRequestType.VALUE_GET);
            assertThat(((TestStateFuture<String>) getRequest.getFuture()).getCompletedResult())
                    .isEqualTo(null);
        }
        forStStateExecutor.shutdown();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private <K, V, R> StateRequest<?, ?, ?> buildStateRequest(
            InternalKeyedState<K, V> innerTable,
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
}
