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

import org.junit.jupiter.api.Test;
import org.rocksdb.WriteOptions;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.state.forst.ForStWriteBatchOperation.PutRequest;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link ForStWriteBatchOperation}. */
public class ForStWriteBatchOperationTest extends ForStDBOperationTestBase {

    @Test
    public void testValueStateWriteBatch() throws Exception {
        ForStValueState<Integer, String> valueState1 = buildForStValueState("test-write-batch-1");
        ForStValueState<Integer, String> valueState2 = buildForStValueState("test-write-batch-2");
        List<PutRequest<ContextKey<Integer>, String>> batchPutRequest = new ArrayList<>();
        int keyNum = 100;
        for (int i = 0; i < keyNum; i++) {
            batchPutRequest.add(
                    PutRequest.of(
                            buildContextKey(i),
                            String.valueOf(i),
                            ((i % 2 == 0) ? valueState1 : valueState2)));
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStWriteBatchOperation<ContextKey<Integer>, String> writeBatchOperation =
                new ForStWriteBatchOperation<>(db, batchPutRequest, new WriteOptions(), executor);
        writeBatchOperation.process().get();

        // check data correctness
        for (PutRequest<ContextKey<Integer>, String> request : batchPutRequest) {
            ForStInnerTable<ContextKey<Integer>, String> table = request.table;
            byte[] keyBytes = table.serializeKey(request.key);
            byte[] valueBytes = db.get(table.getColumnFamilyHandle(), keyBytes);
            assertThat(table.deserializeValue(valueBytes)).isEqualTo(request.value);
        }
    }

    @Test
    public void testWriteBatchWithNullValue() throws Exception {
        ForStValueState<Integer, String> valueState = buildForStValueState("test-write-batch");
        List<PutRequest<ContextKey<Integer>, String>> batchPutRequest = new ArrayList<>();
        // 1. write some data without null value
        int keyNum = 100;
        for (int i = 0; i < keyNum; i++) {
            batchPutRequest.add(PutRequest.of(buildContextKey(i), String.valueOf(i), valueState));
        }
        ExecutorService executor = Executors.newFixedThreadPool(2);
        ForStWriteBatchOperation<ContextKey<Integer>, String> writeBatchOperation =
                new ForStWriteBatchOperation<>(db, batchPutRequest, new WriteOptions(), executor);
        writeBatchOperation.process().get();

        // 2. update data with null value
        batchPutRequest.clear();
        for (int i = 0; i < keyNum; i++) {
            if (i % 8 == 0) {
                batchPutRequest.add(PutRequest.of(buildContextKey(i), null, valueState));
            } else {
                batchPutRequest.add(
                        PutRequest.of(buildContextKey(i), String.valueOf(i * 2), valueState));
            }
        }
        ForStWriteBatchOperation<ContextKey<Integer>, String> writeBatchOperation2 =
                new ForStWriteBatchOperation<>(db, batchPutRequest, new WriteOptions(), executor);
        writeBatchOperation2.process().get();

        // 3.  check data correctness
        for (PutRequest<ContextKey<Integer>, String> request : batchPutRequest) {
            ForStInnerTable<ContextKey<Integer>, String> table = request.table;
            byte[] keyBytes = table.serializeKey(request.key);
            byte[] valueBytes = db.get(table.getColumnFamilyHandle(), keyBytes);
            String value = (valueBytes == null) ? null : table.deserializeValue(valueBytes);
            assertThat(value).isEqualTo(request.value);
        }
    }
}
