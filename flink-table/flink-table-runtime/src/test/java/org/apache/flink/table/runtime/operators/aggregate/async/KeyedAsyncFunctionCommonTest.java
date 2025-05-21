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

package org.apache.flink.table.runtime.operators.aggregate.async;

import org.apache.flink.streaming.api.functions.async.CollectionSupplier;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableList;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link KeyedAsyncFunctionCommon}. */
public class KeyedAsyncFunctionCommonTest {

    private static final RowData KEY1 = GenericRowData.of(1);

    private static final RowData KEY2 = GenericRowData.of(2);

    private TestOpenContext openContext;

    @BeforeEach
    public void setUp() {
        openContext = new TestOpenContext();
        openContext.setCurrentKey(KEY1);
    }

    @Test
    public void testOneCallback() throws Exception {
        TestKeyedAsyncFunctionCommon function = new TestKeyedAsyncFunctionCommon();
        function.open(openContext);
        TestResultFuture resultFuture = new TestResultFuture();
        doCall(function, 123, resultFuture);
        function.results.get(0).complete(321);
        assertThat(resultFuture.collection).containsExactly(321);
    }

    @Test
    public void testMultipleCallbacks() throws Exception {
        TestKeyedAsyncFunctionCommon function = new TestKeyedAsyncFunctionCommon();
        function.open(openContext);
        TestResultFuture resultFuture1 = new TestResultFuture();
        doCall(function, 123, resultFuture1);
        TestResultFuture resultFuture2 = new TestResultFuture();
        doCall(function, 456, resultFuture2);
        TestResultFuture resultFuture3 = new TestResultFuture();
        doCall(function, 789, resultFuture3);
        // Each one synchronously adds the next in line
        assertThat(function.results).hasSize(1);
        function.results.get(0).complete(321);
        assertThat(function.results).hasSize(2);
        function.results.get(1).complete(654);
        assertThat(function.results).hasSize(3);
        function.results.get(2).complete(987);
        assertThat(resultFuture1.collection).containsExactly(321);
        assertThat(resultFuture2.collection).containsExactly(654);
        assertThat(resultFuture3.collection).containsExactly(987);
    }

    @Test
    public void testMultipleCallbacksMultipleKeys() throws Exception {
        TestKeyedAsyncFunctionCommon function = new TestKeyedAsyncFunctionCommon();
        function.open(openContext);
        openContext.setCurrentKey(KEY1);
        TestResultFuture resultFuture11 = new TestResultFuture();
        doCall(function, 123, resultFuture11);
        openContext.setCurrentKey(KEY2);
        TestResultFuture resultFuture21 = new TestResultFuture();
        doCall(function, 333, resultFuture21);

        openContext.setCurrentKey(KEY1);
        TestResultFuture resultFuture12 = new TestResultFuture();
        doCall(function, 456, resultFuture12);
        openContext.setCurrentKey(KEY2);
        TestResultFuture resultFuture22 = new TestResultFuture();
        doCall(function, 444, resultFuture22);

        openContext.setCurrentKey(KEY1);
        TestResultFuture resultFuture13 = new TestResultFuture();
        doCall(function, 789, resultFuture13);
        openContext.setCurrentKey(KEY2);
        TestResultFuture resultFuture23 = new TestResultFuture();
        doCall(function, 555, resultFuture23);

        // Each one synchronously adds the next in line
        assertThat(function.results).hasSize(2);
        function.results.get(0).complete(321);
        function.results.get(1).complete(3333);
        assertThat(function.results).hasSize(4);
        function.results.get(2).complete(654);
        function.results.get(3).complete(4444);
        assertThat(function.results).hasSize(6);
        function.results.get(4).complete(987);
        function.results.get(5).complete(5555);
        assertThat(resultFuture11.collection).containsExactly(321);
        assertThat(resultFuture21.collection).containsExactly(3333);
        assertThat(resultFuture12.collection).containsExactly(654);
        assertThat(resultFuture22.collection).containsExactly(4444);
        assertThat(resultFuture13.collection).containsExactly(987);
        assertThat(resultFuture23.collection).containsExactly(5555);
    }

    @Test
    public void testError() throws Exception {
        TestKeyedAsyncFunctionCommon function = new TestKeyedAsyncFunctionCommon();
        function.open(openContext);
        TestResultFuture resultFuture1 = new TestResultFuture();
        doCall(function, 123, resultFuture1);
        TestResultFuture resultFuture2 = new TestResultFuture();
        doCall(function, 456, resultFuture2);
        TestResultFuture resultFuture3 = new TestResultFuture();
        doCall(function, 789, resultFuture3);
        // Each one synchronously adds the next in line
        assertThat(function.results).hasSize(1);
        function.results.get(0).complete(321);
        assertThat(function.results).hasSize(2);
        function.results.get(1).completeExceptionally(new RuntimeException("Error!"));
        assertThat(function.results).hasSize(3);
        function.results.get(2).complete(987);
        assertThat(resultFuture1.collection).containsExactly(321);
        assertThat(resultFuture2.throwable).hasMessageContaining("Error!");
        assertThat(resultFuture3.collection).containsExactly(987);
    }

    private void doCall(
            TestKeyedAsyncFunctionCommon function, int input, ResultFuture<Integer> resultFuture)
            throws Exception {
        function.asyncInvoke(input, resultFuture);
    }

    private static class TestKeyedAsyncFunctionCommon
            extends KeyedAsyncFunctionCommon<Integer, Integer, Integer> {

        private List<CompletableFuture<Integer>> results = new ArrayList<>();

        @Override
        public void asyncInvokeProtected(Integer input, ResultFuture<Integer> resultFuture)
                throws Exception {
            // Fake doing an rpc
            CompletableFuture<Integer> future = new CompletableFuture<>();
            results.add(future);
            handleResponseForAsyncInvoke(
                    future,
                    resultFuture::completeExceptionally,
                    r -> {
                        resultFuture.complete(ImmutableList.of(r));
                    });
        }
    }

    private static class TestResultFuture implements ResultFuture<Integer> {

        private Collection<Integer> collection;
        private Throwable throwable;

        @Override
        public void complete(Collection<Integer> collection) {
            this.collection = collection;
        }

        @Override
        public void completeExceptionally(Throwable throwable) {
            this.throwable = throwable;
        }

        @Override
        public void complete(CollectionSupplier<Integer> supplier) {
            try {
                this.collection = supplier.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
