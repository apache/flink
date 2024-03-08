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

package org.apache.flink.streaming.api.operators;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

/** Unit test for {@link MapPartitionIterator}. */
class MapPartitionIteratorTest {

    @Test
    void testRegisterUDF() throws ExecutionException, InterruptedException {
        MapPartitionIterator<String> iterator = createMapPartitionIterator();
        CompletableFuture<Boolean> result = new CompletableFuture<>();
        iterator.registerUDF(stringIterator -> result.complete(true));
        assertThat(result.get()).isTrue();
    }

    @Test
    void testAddRecord() throws ExecutionException, InterruptedException {
        MapPartitionIterator<String> iterator = createMapPartitionIterator();
        CompletableFuture<List<String>> result = new CompletableFuture<>();
        iterator.registerUDF(
                stringIterator -> {
                    List<String> strings = new ArrayList<>();
                    int index = 0;
                    while (stringIterator.hasNext()) {
                        strings.add(stringIterator.next() + index++);
                    }
                    result.complete(strings);
                });
        iterator.addRecord("Test");
        iterator.addRecord("Test");
        iterator.addRecord("Test");
        iterator.close();
        List<String> strings = result.get();
        assertThat(strings.size()).isEqualTo(3);
        assertThat(strings.get(0)).isEqualTo("Test0");
        assertThat(strings.get(1)).isEqualTo("Test1");
        assertThat(strings.get(2)).isEqualTo("Test2");
    }

    @Test
    void testClose() throws ExecutionException, InterruptedException {
        MapPartitionIterator<String> iterator = createMapPartitionIterator();
        CompletableFuture<?> result = new CompletableFuture<>();
        iterator.registerUDF(
                ignored -> {
                    result.complete(null);
                });
        iterator.addRecord("Test");
        assertThat(iterator.hasNext()).isTrue();
        result.get();
        assertThat(result).isCompleted();
        iterator.close();
        assertThatThrownBy(() -> iterator.addRecord("Test"))
                .isInstanceOf(IllegalStateException.class);
    }

    private MapPartitionIterator<String> createMapPartitionIterator() {
        return new MapPartitionIterator<>();
    }
}
