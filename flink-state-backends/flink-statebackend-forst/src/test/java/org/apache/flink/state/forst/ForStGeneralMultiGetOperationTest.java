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
import org.apache.flink.runtime.state.VoidNamespace;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link ForStGeneralMultiGetOperation}. */
class ForStGeneralMultiGetOperationTest extends ForStDBOperationTestBase {

    @Test
    void testValueStateMultiGet() throws Exception {
        ForStValueState<Integer, VoidNamespace, String> valueState1 =
                buildForStValueState("test-multiGet-1");
        ForStValueState<Integer, VoidNamespace, String> valueState2 =
                buildForStValueState("test-multiGet-2");
        List<ForStDBGetRequest<?, ?, ?, ?>> batchGetRequest = new ArrayList<>();
        List<Tuple2<String, TestAsyncFuture<String>>> resultCheckList = new ArrayList<>();

        int keyNum = 1000;
        for (int i = 0; i < keyNum; i++) {
            TestAsyncFuture<String> future = new TestAsyncFuture<>();
            ForStValueState<Integer, VoidNamespace, String> table =
                    ((i % 2 == 0) ? valueState1 : valueState2);
            ForStDBSingleGetRequest<Integer, VoidNamespace, String> request =
                    new ForStDBSingleGetRequest<>(buildContextKey(i), table, future);
            batchGetRequest.add(request);

            String value = (i % 10 != 0 ? String.valueOf(i) : null);
            resultCheckList.add(Tuple2.of(value, future));
            if (value == null) {
                continue;
            }
            byte[] keyBytes = request.buildSerializedKey();
            byte[] valueBytes = table.serializeValue(value);
            db.put(request.getColumnFamilyHandle(), keyBytes, valueBytes);
        }

        ExecutorService executor = Executors.newFixedThreadPool(3);
        ForStGeneralMultiGetOperation generalMultiGetOperation =
                new ForStGeneralMultiGetOperation(db, batchGetRequest, executor, 3, null);
        generalMultiGetOperation.process().get();

        for (Tuple2<String, TestAsyncFuture<String>> tuple : resultCheckList) {
            assertThat(tuple.f1.getCompletedResult()).isEqualTo(tuple.f0);
        }

        executor.shutdownNow();
    }

    @Test
    void testListStateMultiGet() throws Exception {
        ForStListState<Integer, VoidNamespace, String> listState1 =
                buildForStListState("test-multiGet-1");
        ForStListState<Integer, VoidNamespace, String> listState2 =
                buildForStListState("test-multiGet-2");
        List<ForStDBGetRequest<?, ?, ?, ?>> batchGetRequest = new ArrayList<>();
        List<Tuple2<List<String>, TestAsyncFuture<StateIterator<String>>>> resultCheckList =
                new ArrayList<>();

        int keyNum = 1000;
        for (int i = 0; i < keyNum; i++) {
            TestAsyncFuture<StateIterator<String>> future = new TestAsyncFuture<>();
            ForStListState<Integer, VoidNamespace, String> table =
                    ((i % 2 == 0) ? listState1 : listState2);
            ForStDBListGetRequest<Integer, VoidNamespace, String> request =
                    new ForStDBListGetRequest<>(buildContextKey(i), table, future);
            batchGetRequest.add(request);

            List<String> value = new ArrayList<>();
            value.add(String.valueOf(i));
            value.add(String.valueOf(i + 1));

            resultCheckList.add(Tuple2.of(value, future));
            byte[] keyBytes = request.buildSerializedKey();
            byte[] valueBytes = table.serializeValue(value);
            db.put(request.getColumnFamilyHandle(), keyBytes, valueBytes);
        }

        ExecutorService executor = Executors.newFixedThreadPool(4);
        ForStGeneralMultiGetOperation generalMultiGetOperation =
                new ForStGeneralMultiGetOperation(db, batchGetRequest, executor);
        generalMultiGetOperation.process().get();

        for (Tuple2<List<String>, TestAsyncFuture<StateIterator<String>>> tuple : resultCheckList) {
            HashSet<String> expected = new HashSet<>(tuple.f0);
            tuple.f1
                    .getCompletedResult()
                    .onNext(
                            (e) -> {
                                assertThat(expected.remove(e)).isTrue();
                            });
            assertThat(expected).isEmpty();
        }
        executor.shutdownNow();
    }

    @Test
    void testMapStateMultiGet() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState1 =
                buildForStMapState("map-multiGet-1");
        ForStMapState<Integer, VoidNamespace, String, String> mapState2 =
                buildForStMapState("map-multiGet-2");
        List<ForStDBGetRequest<?, ?, ?, ?>> batchGetRequest = new ArrayList<>();
        List<Tuple2<String, TestAsyncFuture<String>>> resultCheckList = new ArrayList<>();

        int keyNum = 1000;
        for (int i = 0; i < keyNum; i++) {
            TestAsyncFuture<String> future = new TestAsyncFuture<>();
            ForStMapState<Integer, VoidNamespace, String, String> table =
                    ((i % 2 == 0) ? mapState1 : mapState2);
            ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(i);
            contextKey.setUserKey(String.valueOf(i));
            ForStDBGetRequest<Integer, VoidNamespace, String, String> request =
                    new ForStDBSingleGetRequest<>(contextKey, table, future);
            batchGetRequest.add(request);

            String value = (i % 10 != 0 ? String.valueOf(i) : null);
            resultCheckList.add(Tuple2.of(value, future));
            if (value == null) {
                continue;
            }
            byte[] keyBytes = request.buildSerializedKey();
            byte[] valueBytes = table.serializeValue(value);
            db.put(request.getColumnFamilyHandle(), keyBytes, valueBytes);
        }

        ExecutorService executor = Executors.newFixedThreadPool(4);
        ForStGeneralMultiGetOperation generalMultiGetOperation =
                new ForStGeneralMultiGetOperation(db, batchGetRequest, executor);
        generalMultiGetOperation.process().get();

        for (Tuple2<String, TestAsyncFuture<String>> tuple : resultCheckList) {
            assertThat(tuple.f1.getCompletedResult()).isEqualTo(tuple.f0);
        }

        executor.shutdownNow();
    }

    @Test
    void testMapStateEmpty() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("map-is-empty");
        for (int i = 0; i < 10; i++) {
            ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
            contextKey.setUserKey(String.valueOf(i));
            String value = String.valueOf(i);
            byte[] keyBytes = mapState.serializeKey(contextKey);
            byte[] valueBytes = mapState.serializeValue(value);
            db.put(mapState.getColumnFamilyHandle(), keyBytes, valueBytes);
        }
        TestAsyncFuture<Boolean> future = new TestAsyncFuture<>();
        List<ForStDBGetRequest<?, ?, ?, ?>> batchGetRequest = new ArrayList<>();
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
        ForStDBGetRequest<Integer, VoidNamespace, String, Boolean> request1 =
                new ForStDBMapCheckRequest<>(contextKey, mapState, future, true);
        batchGetRequest.add(request1);

        TestAsyncFuture<Boolean> future2 = new TestAsyncFuture<>();
        ContextKey<Integer, VoidNamespace> contextKey2 = buildContextKey(2);
        ForStDBGetRequest<Integer, VoidNamespace, String, Boolean> request2 =
                new ForStDBMapCheckRequest<>(contextKey2, mapState, future2, true);
        batchGetRequest.add(request2);

        TestAsyncFuture<Boolean> future3 = new TestAsyncFuture<>();
        ContextKey<Integer, VoidNamespace> contextKey3 = buildContextKey(1);
        contextKey3.setUserKey("10");
        ForStDBGetRequest<Integer, VoidNamespace, String, Boolean> request3 =
                new ForStDBMapCheckRequest<>(contextKey3, mapState, future3, false);
        batchGetRequest.add(request3);

        TestAsyncFuture<Boolean> future4 = new TestAsyncFuture<>();
        ContextKey<Integer, VoidNamespace> contextKey4 = buildContextKey(1);
        contextKey4.setUserKey("1");
        ForStDBGetRequest<Integer, VoidNamespace, String, Boolean> request4 =
                new ForStDBMapCheckRequest<>(contextKey4, mapState, future4, false);
        batchGetRequest.add(request4);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStGeneralMultiGetOperation generalMultiGetOperation =
                new ForStGeneralMultiGetOperation(db, batchGetRequest, executor);
        generalMultiGetOperation.process().get();

        // key 1 is not empty
        assertThat(future.getCompletedResult()).isFalse();
        // key 2 is empty
        assertThat(future2.getCompletedResult()).isTrue();

        // key 1#10 not exists
        assertThat(future3.getCompletedResult()).isFalse();
        // key 1#1 exists
        assertThat(future4.getCompletedResult()).isTrue();
    }
}
