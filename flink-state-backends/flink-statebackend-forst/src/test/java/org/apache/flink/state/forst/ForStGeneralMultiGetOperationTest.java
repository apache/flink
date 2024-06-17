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

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link ForStGeneralMultiGetOperation}. */
public class ForStGeneralMultiGetOperationTest extends ForStDBOperationTestBase {

    @Test
    public void testValueStateMultiGet() throws Exception {
        ForStValueState<Integer, String> valueState1 = buildForStValueState("test-multiGet-1");
        ForStValueState<Integer, String> valueState2 = buildForStValueState("test-multiGet-2");
        List<ForStDBGetRequest<?, ?>> batchGetRequest = new ArrayList<>();
        List<Tuple2<String, TestStateFuture<String>>> resultCheckList = new ArrayList<>();

        int keyNum = 1000;
        for (int i = 0; i < keyNum; i++) {
            TestStateFuture<String> future = new TestStateFuture<>();
            ForStValueState<Integer, String> table = ((i % 2 == 0) ? valueState1 : valueState2);
            ForStDBGetRequest<ContextKey<Integer>, String> request =
                    ForStDBGetRequest.of(buildContextKey(i), table, future);
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

        for (Tuple2<String, TestStateFuture<String>> tuple : resultCheckList) {
            assertThat(tuple.f1.getCompletedResult()).isEqualTo(tuple.f0);
        }

        executor.shutdownNow();
    }
}
