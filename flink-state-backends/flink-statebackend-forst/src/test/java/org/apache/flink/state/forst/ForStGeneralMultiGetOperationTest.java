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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.state.forst.ForStGeneralMultiGetOperation.GetRequest;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link ForStGeneralMultiGetOperation}. */
public class ForStGeneralMultiGetOperationTest extends ForStDBOperationTestBase {

    @Test
    public void testValueStateMultiGet() throws Exception {
        ForStValueState<Integer, String> valueState1 = buildForStValueState("test-multiGet-1");
        ForStValueState<Integer, String> valueState2 = buildForStValueState("test-multiGet-2");
        List<GetRequest<ContextKey<Integer>, String>> batchGetRequest = new ArrayList<>();
        List<String> resultCheckList = new ArrayList<>();

        int keyNum = 1000;
        for (int i = 0; i < keyNum; i++) {
            GetRequest<ContextKey<Integer>, String> request =
                    GetRequest.of(buildContextKey(i), ((i % 2 == 0) ? valueState1 : valueState2));
            batchGetRequest.add(request);
            String value = (i % 10 != 0 ? String.valueOf(i) : null);
            resultCheckList.add(value);
            if (value == null) {
                continue;
            }
            byte[] keyBytes = request.table.serializeKey(request.key);
            byte[] valueBytes = request.table.serializeValue(value);
            db.put(request.table.getColumnFamilyHandle(), keyBytes, valueBytes);
        }

        ExecutorService executor = Executors.newFixedThreadPool(4);
        ForStGeneralMultiGetOperation<ContextKey<Integer>, String> generalMultiGetOperation =
                new ForStGeneralMultiGetOperation<>(db, batchGetRequest, executor);
        List<String> result = generalMultiGetOperation.process().get();

        assertThat(result).isEqualTo(resultCheckList);

        executor.shutdownNow();
    }
}
