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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.VoidNamespace;

import org.forstdb.WriteOptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit test for {@link ForStWriteBatchOperation}. */
public class ForStWriteBatchOperationTest extends ForStDBOperationTestBase {

    @Test
    public void testValueStateWriteBatch() throws Exception {
        ForStValueState<Integer, VoidNamespace, String> valueState1 =
                buildForStValueState("test-write-batch-1");
        ForStValueState<Integer, VoidNamespace, String> valueState2 =
                buildForStValueState("test-write-batch-2");
        List<ForStDBPutRequest<?, ?, ?>> batchPutRequest = new ArrayList<>();
        int keyNum = 100;
        for (int i = 0; i < keyNum; i++) {
            batchPutRequest.add(
                    ForStDBPutRequest.of(
                            buildContextKey(i),
                            String.valueOf(i),
                            ((i % 2 == 0) ? valueState1 : valueState2),
                            new TestAsyncFuture<>()));
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStWriteBatchOperation writeBatchOperation =
                new ForStWriteBatchOperation(db, batchPutRequest, new WriteOptions(), executor);
        writeBatchOperation.process().get();

        // check data correctness
        for (ForStDBPutRequest<?, ?, ?> request : batchPutRequest) {
            byte[] keyBytes = request.buildSerializedKey();
            byte[] valueBytes = db.get(request.getColumnFamilyHandle(), keyBytes);
            assertArrayEquals(valueBytes, request.buildSerializedValue());
        }
    }

    @Test
    public void testWriteBatchWithNullValue() throws Exception {
        ForStValueState<Integer, VoidNamespace, String> valueState =
                buildForStValueState("test-write-batch");
        List<ForStDBPutRequest<?, ?, ?>> batchPutRequest = new ArrayList<>();
        // 1. write some data without null value
        int keyNum = 100;
        for (int i = 0; i < keyNum; i++) {
            batchPutRequest.add(
                    ForStDBPutRequest.of(
                            buildContextKey(i),
                            String.valueOf(i),
                            valueState,
                            new TestAsyncFuture<>()));
        }
        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStWriteBatchOperation writeBatchOperation =
                new ForStWriteBatchOperation(db, batchPutRequest, new WriteOptions(), executor);
        writeBatchOperation.process().get();

        // 2. update data with null value
        batchPutRequest.clear();
        for (int i = 0; i < keyNum; i++) {
            if (i % 8 == 0) {
                batchPutRequest.add(
                        ForStDBPutRequest.of(
                                buildContextKey(i), null, valueState, new TestAsyncFuture<>()));
            } else {
                batchPutRequest.add(
                        ForStDBPutRequest.of(
                                buildContextKey(i),
                                String.valueOf(i * 2),
                                valueState,
                                new TestAsyncFuture<>()));
            }
        }
        ForStWriteBatchOperation writeBatchOperation2 =
                new ForStWriteBatchOperation(db, batchPutRequest, new WriteOptions(), executor);
        writeBatchOperation2.process().get();

        // 3.  check data correctness
        for (ForStDBPutRequest<?, ?, ?> request : batchPutRequest) {
            byte[] keyBytes = request.buildSerializedKey();
            byte[] valueBytes = db.get(request.getColumnFamilyHandle(), keyBytes);
            if (valueBytes == null) {
                assertTrue(request.valueIsNull());
            } else {
                assertArrayEquals(valueBytes, request.buildSerializedValue());
            }
        }
    }

    @Test
    public void testListStateWriteBatch() throws Exception {
        ForStListState<Integer, VoidNamespace, String> listState =
                buildForStListState("test-write-batch-1");
        List<ForStDBPutRequest<?, ?, ?>> batchPutRequest = new ArrayList<>();
        ArrayList<Tuple2<ForStDBPutRequest<?, ?, ?>, List<String>>> resultCheckList =
                new ArrayList<>();

        // update
        int keyNum = 100;
        for (int i = 0; i < keyNum; i++) {
            List<String> value = new ArrayList<>();
            value.add(String.valueOf(i));
            value.add(String.valueOf(i + 1));

            ForStDBPutRequest<Integer, VoidNamespace, List<String>> request =
                    ForStDBPutRequest.of(
                            buildContextKey(i), value, listState, new TestAsyncFuture<>());

            resultCheckList.add(Tuple2.of(request, value));
            batchPutRequest.add(request);
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStWriteBatchOperation writeBatchOperation =
                new ForStWriteBatchOperation(db, batchPutRequest, new WriteOptions(), executor);
        writeBatchOperation.process().get();

        // check data correctness
        for (Tuple2<ForStDBPutRequest<?, ?, ?>, List<String>> tuple : resultCheckList) {
            byte[] keyBytes = tuple.f0.buildSerializedKey();
            byte[] valueBytes = db.get(tuple.f0.getColumnFamilyHandle(), keyBytes);
            assertThat(listState.deserializeValue(valueBytes)).isEqualTo(tuple.f1);
        }

        // add or addall with update
        batchPutRequest.clear();
        resultCheckList.clear();
        for (int i = 0; i < keyNum / 2; i++) {
            List<String> value = new ArrayList<>();
            value.add(String.valueOf(i));
            value.add(String.valueOf(i + 1));

            List<String> deltaValue = new ArrayList<>();
            deltaValue.add(String.valueOf(i * 2));
            deltaValue.add(String.valueOf(i * 3));

            value.addAll(deltaValue);

            ForStDBPutRequest<Integer, VoidNamespace, List<String>> request =
                    ForStDBPutRequest.ofMerge(
                            buildContextKey(i), deltaValue, listState, new TestAsyncFuture<>());

            resultCheckList.add(Tuple2.of(request, value));
            batchPutRequest.add(request);
        }
        for (int i = keyNum / 2; i < keyNum; i++) {
            List<String> value = new ArrayList<>();
            value.add(String.valueOf(i));
            value.add(String.valueOf(i + 1));

            ForStDBPutRequest<Integer, VoidNamespace, List<String>> request =
                    ForStDBPutRequest.of(
                            buildContextKey(i), value, listState, new TestAsyncFuture<>());

            resultCheckList.add(Tuple2.of(request, value));
            batchPutRequest.add(request);
        }

        writeBatchOperation =
                new ForStWriteBatchOperation(db, batchPutRequest, new WriteOptions(), executor);
        writeBatchOperation.process().get();

        // check data correctness
        for (Tuple2<ForStDBPutRequest<?, ?, ?>, List<String>> tuple : resultCheckList) {
            byte[] keyBytes = tuple.f0.buildSerializedKey();
            byte[] valueBytes = db.get(tuple.f0.getColumnFamilyHandle(), keyBytes);
            assertThat(listState.deserializeValue(valueBytes)).isEqualTo(tuple.f1);
        }
    }

    @Test
    void testMapStateWriteBatch() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState1 =
                buildForStMapState("test-write-batch-1");
        ForStMapState<Integer, VoidNamespace, String, String> maoState2 =
                buildForStMapState("test-write-batch-2");

        List<ForStDBPutRequest<?, ?, ?>> batchPutRequest = new ArrayList<>();
        int keyNum = 100;
        for (int i = 0; i < keyNum; i++) {
            ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(i);
            contextKey.setUserKey(String.valueOf(i));
            batchPutRequest.add(
                    ForStDBPutRequest.of(
                            contextKey,
                            String.valueOf(i),
                            ((i % 2 == 0) ? mapState1 : mapState1),
                            new TestAsyncFuture<>()));
        }

        HashMap<String, String> map = new HashMap<>(100);
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(101);
        for (int j = 0; j < 100; j++) {
            map.put("test-" + j, "test-" + j);
        }
        batchPutRequest.add(
                new ForStDBBunchPutRequest<>(contextKey, map, mapState1, new TestAsyncFuture<>()));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStWriteBatchOperation writeBatchOperation =
                new ForStWriteBatchOperation(db, batchPutRequest, new WriteOptions(), executor);
        writeBatchOperation.process().get();

        // check data correctness
        for (ForStDBPutRequest<?, ?, ?> request : batchPutRequest) {
            if (request instanceof ForStDBBunchPutRequest) {
                ForStDBBunchPutRequest bunchPutRequest = (ForStDBBunchPutRequest) request;
                for (Object entry : bunchPutRequest.getBunchValue().entrySet()) {
                    Map.Entry<String, String> en = (Map.Entry<String, String>) entry;
                    byte[] keyBytes = bunchPutRequest.buildSerializedKey(en.getKey());
                    byte[] valueBytes = db.get(request.getColumnFamilyHandle(), keyBytes);
                    assertArrayEquals(
                            valueBytes, bunchPutRequest.buildSerializedValue(en.getValue()));
                }
            } else {
                byte[] keyBytes = request.buildSerializedKey();
                byte[] valueBytes = db.get(request.getColumnFamilyHandle(), keyBytes);
                assertArrayEquals(valueBytes, request.buildSerializedValue());
            }
        }
    }

    @Test
    public void testMapClear() throws Exception {
        ForStMapState<Integer, VoidNamespace, String, String> mapState =
                buildForStMapState("test-write-batch-1");

        List<ForStDBPutRequest<?, ?, ?>> batchPutRequest = new ArrayList<>();
        int keyNum = 100;
        for (int i = 0; i < keyNum; i++) {
            ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);
            contextKey.setUserKey(String.valueOf(i));
            batchPutRequest.add(
                    ForStDBPutRequest.of(
                            contextKey, String.valueOf(i), mapState, new TestAsyncFuture<>()));
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStWriteBatchOperation writeBatchOperation =
                new ForStWriteBatchOperation(db, batchPutRequest, new WriteOptions(), executor);
        writeBatchOperation.process().get();

        // before clear, all data under primary key 1 should exit
        for (ForStDBPutRequest<?, ?, ?> request : batchPutRequest) {
            byte[] keyBytes = request.buildSerializedKey();
            byte[] valueBytes = db.get(request.getColumnFamilyHandle(), keyBytes);
            assertArrayEquals(valueBytes, request.buildSerializedValue());
        }
        ContextKey<Integer, VoidNamespace> contextKey = buildContextKey(1);

        List<ForStDBPutRequest<?, ?, ?>> batchPutRequest1 = new ArrayList<>();
        batchPutRequest1.add(
                new ForStDBBunchPutRequest<>(contextKey, null, mapState, new TestAsyncFuture<>()));

        writeBatchOperation =
                new ForStWriteBatchOperation(db, batchPutRequest1, new WriteOptions(), executor);
        writeBatchOperation.process().get();

        for (ForStDBPutRequest<?, ?, ?> request : batchPutRequest) {
            byte[] keyBytes = request.buildSerializedKey();
            byte[] valueBytes = db.get(request.getColumnFamilyHandle(), keyBytes);
            assertThat(valueBytes).isNull();
        }
    }
}
