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

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.asyncprocessing.AsyncExecutionController;
import org.apache.flink.runtime.asyncprocessing.MockStateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequest;
import org.apache.flink.runtime.asyncprocessing.StateRequestContainer;
import org.apache.flink.runtime.asyncprocessing.StateRequestType;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for {@link AbstractReducingState}. */
public class AbstractReducingStateTest extends AbstractKeyedStateTestBase {

    @Test
    @SuppressWarnings({"unchecked"})
    public void testEachOperation() {
        ReduceFunction<Integer> reducer = Integer::sum;
        ReducingStateDescriptor<Integer> descriptor =
                new ReducingStateDescriptor<>("testState", reducer, BasicTypeInfo.INT_TYPE_INFO);
        AbstractReducingState<String, Void, Integer> reducingState =
                new AbstractReducingState<>(aec, descriptor);
        aec.setCurrentContext(aec.buildContext("test", "test"));

        reducingState.asyncClear();
        validateRequestRun(reducingState, StateRequestType.CLEAR, null, 0);

        reducingState.asyncGet();
        validateRequestRun(reducingState, StateRequestType.REDUCING_GET, null, 0);

        reducingState.asyncAdd(1);
        validateRequestRun(reducingState, StateRequestType.REDUCING_GET, null, 1);
        validateRequestRun(reducingState, StateRequestType.REDUCING_ADD, 1, 0);

        reducingState.clear();
        validateRequestRun(reducingState, StateRequestType.CLEAR, null, 0);

        reducingState.get();
        validateRequestRun(reducingState, StateRequestType.REDUCING_GET, null, 0);

        reducingState.add(1);
        validateRequestRun(reducingState, StateRequestType.REDUCING_GET, null, 1);
        validateRequestRun(reducingState, StateRequestType.REDUCING_ADD, 1, 0);
    }

    @Test
    public void testMergeNamespace() throws Exception {
        ReduceFunction<Integer> reducer = Integer::sum;
        ReducingStateDescriptor<Integer> descriptor =
                new ReducingStateDescriptor<>("testState", reducer, BasicTypeInfo.INT_TYPE_INFO);
        AsyncExecutionController<String> aec =
                new AsyncExecutionController<>(
                        new SyncMailboxExecutor(),
                        (a, b) -> {},
                        new ReducingStateExecutor(),
                        1,
                        100,
                        10000,
                        1,
                        null);
        AbstractReducingState<String, String, Integer> reducingState =
                new AbstractReducingState<>(aec, descriptor);
        aec.setCurrentContext(aec.buildContext("test", "test"));
        aec.setCurrentNamespaceForState(reducingState, "1");
        reducingState.asyncAdd(1);
        aec.drainInflightRecords(0);
        assertThat(ReducingStateExecutor.hashMap.size()).isEqualTo(1);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "1"))).isEqualTo(1);
        aec.setCurrentNamespaceForState(reducingState, "2");
        reducingState.asyncAdd(2);
        aec.drainInflightRecords(0);
        assertThat(ReducingStateExecutor.hashMap.size()).isEqualTo(2);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "1"))).isEqualTo(1);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "2"))).isEqualTo(2);
        aec.setCurrentNamespaceForState(reducingState, "3");
        reducingState.asyncAdd(3);
        aec.drainInflightRecords(0);
        assertThat(ReducingStateExecutor.hashMap.size()).isEqualTo(3);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "1"))).isEqualTo(1);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "2"))).isEqualTo(2);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "3"))).isEqualTo(3);

        List<String> sources = new ArrayList<>(Arrays.asList("1", "2", "3"));
        reducingState.asyncMergeNamespaces("0", sources);
        aec.drainInflightRecords(0);
        assertThat(ReducingStateExecutor.hashMap.size()).isEqualTo(1);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "0"))).isEqualTo(6);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "1"))).isNull();
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "2"))).isNull();
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "3"))).isNull();

        aec.setCurrentNamespaceForState(reducingState, "4");
        reducingState.asyncAdd(4);
        aec.drainInflightRecords(0);
        assertThat(ReducingStateExecutor.hashMap.size()).isEqualTo(2);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "0"))).isEqualTo(6);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "4"))).isEqualTo(4);

        List<String> sources1 = new ArrayList<>(Arrays.asList("4"));
        reducingState.asyncMergeNamespaces("0", sources1);
        aec.drainInflightRecords(0);

        assertThat(ReducingStateExecutor.hashMap.size()).isEqualTo(1);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "0"))).isEqualTo(10);
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "1"))).isNull();
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "2"))).isNull();
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "3"))).isNull();
        assertThat(ReducingStateExecutor.hashMap.get(Tuple2.of("test", "4"))).isNull();
    }

    static class ReducingStateExecutor implements StateExecutor {

        private static final HashMap<Tuple2<String, String>, Integer> hashMap = new HashMap<>();

        public ReducingStateExecutor() {
            hashMap.clear();
        }

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public CompletableFuture<Void> executeBatchRequests(
                StateRequestContainer stateRequestContainer) {
            Preconditions.checkArgument(stateRequestContainer instanceof MockStateRequestContainer);
            CompletableFuture<Void> future = new CompletableFuture<>();
            for (StateRequest request :
                    ((MockStateRequestContainer) stateRequestContainer).getStateRequestList()) {
                if (request.getRequestType() == StateRequestType.REDUCING_GET) {
                    String key = (String) request.getRecordContext().getKey();
                    String namespace = (String) request.getNamespace();
                    Integer val = hashMap.get(Tuple2.of(key, namespace));
                    request.getFuture().complete(val);
                } else if (request.getRequestType() == StateRequestType.REDUCING_ADD) {
                    String key = (String) request.getRecordContext().getKey();
                    String namespace = (String) request.getNamespace();
                    if (request.getPayload() == null) {
                        hashMap.remove(Tuple2.of(key, namespace));
                        request.getFuture().complete(null);
                    } else {
                        hashMap.put(Tuple2.of(key, namespace), (Integer) request.getPayload());
                        request.getFuture().complete(null);
                    }
                } else {
                    throw new UnsupportedOperationException("Unsupported request type");
                }
            }
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
