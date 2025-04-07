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

import org.apache.flink.util.ExceptionUtils;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.api.operators.MapPartitionIterator.DEFAULT_MAX_CACHE_NUM;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link MapPartitionIterator}. */
class MapPartitionIteratorTest {

    private static final String RECORD = "TEST";

    private static final int RECORD_NUMBER = 3;

    @Test
    void testInitialize() throws ExecutionException, InterruptedException {
        CompletableFuture<Object> result = new CompletableFuture<>();
        MapPartitionIterator<String> iterator =
                new MapPartitionIterator<>(stringIterator -> result.complete(null));
        result.get();
        assertThat(result).isCompleted();
        iterator.close();
    }

    @Test
    void testAddRecord() throws ExecutionException, InterruptedException {
        CompletableFuture<List<String>> result = new CompletableFuture<>();
        CompletableFuture<Object> udfFinishTrigger = new CompletableFuture<>();
        MapPartitionIterator<String> iterator =
                new MapPartitionIterator<>(
                        inputIterator -> {
                            List<String> strings = new ArrayList<>();
                            for (int index = 0; index < RECORD_NUMBER; ++index) {
                                strings.add(inputIterator.next());
                            }
                            result.complete(strings);
                            try {
                                udfFinishTrigger.get();
                            } catch (InterruptedException | ExecutionException e) {
                                ExceptionUtils.rethrow(e);
                            }
                        });
        // 1.Test addRecord() when the cache is empty in the MapPartitionIterator.
        addRecordToIterator(RECORD_NUMBER, iterator);
        List<String> results = result.get();
        assertThat(results.size()).isEqualTo(RECORD_NUMBER);
        assertThat(results.get(0)).isEqualTo(RECORD);
        assertThat(results.get(1)).isEqualTo(RECORD);
        assertThat(results.get(2)).isEqualTo(RECORD);
        // 2.Test addRecord() when the cache is full in the MapPartitionIterator.
        addRecordToIterator(DEFAULT_MAX_CACHE_NUM, iterator);
        CompletableFuture<Object> mockedTaskThread1 = new CompletableFuture<>();
        CompletableFuture<List<String>> addRecordFinishIdentifier1 = new CompletableFuture<>();
        mockedTaskThread1.thenRunAsync(
                () -> {
                    iterator.addRecord(RECORD);
                    addRecordFinishIdentifier1.complete(null);
                });
        mockedTaskThread1.complete(null);
        assertThat(addRecordFinishIdentifier1).isNotCompleted();
        iterator.next();
        addRecordFinishIdentifier1.get();
        assertThat(addRecordFinishIdentifier1).isCompleted();
        // 2.Test addRecord() when the udf is finished in the MapPartitionIterator.
        CompletableFuture<Object> mockedTaskThread2 = new CompletableFuture<>();
        CompletableFuture<List<String>> addRecordFinishIdentifier2 = new CompletableFuture<>();
        mockedTaskThread2.thenRunAsync(
                () -> {
                    iterator.addRecord(RECORD);
                    addRecordFinishIdentifier2.complete(null);
                });
        mockedTaskThread2.complete(null);
        assertThat(addRecordFinishIdentifier2).isNotCompleted();
        udfFinishTrigger.complete(null);
        addRecordFinishIdentifier2.get();
        assertThat(addRecordFinishIdentifier2).isCompleted();
        assertThat(udfFinishTrigger).isCompleted();
        iterator.close();
    }

    @Test
    void testHasNext() throws ExecutionException, InterruptedException {
        CompletableFuture<Object> udfTrigger = new CompletableFuture<>();
        CompletableFuture<Object> udfReadIteratorFinishIdentifier = new CompletableFuture<>();
        CompletableFuture<Object> udfFinishTrigger = new CompletableFuture<>();
        MapPartitionIterator<String> iterator =
                new MapPartitionIterator<>(
                        inputIterator -> {
                            try {
                                udfTrigger.get();
                            } catch (InterruptedException | ExecutionException e) {
                                ExceptionUtils.rethrow(e);
                            }
                            for (int index = 0; index < RECORD_NUMBER; ++index) {
                                inputIterator.next();
                            }
                            udfReadIteratorFinishIdentifier.complete(null);
                            try {
                                udfFinishTrigger.get();
                            } catch (InterruptedException | ExecutionException e) {
                                ExceptionUtils.rethrow(e);
                            }
                        });
        // 1.Test hasNext() when the cache is not empty in the MapPartitionIterator.
        addRecordToIterator(RECORD_NUMBER, iterator);
        assertThat(iterator.hasNext()).isTrue();
        // 2.Test hasNext() when the cache is empty in the MapPartitionIterator.
        udfTrigger.complete(null);
        udfReadIteratorFinishIdentifier.get();
        assertThat(udfReadIteratorFinishIdentifier).isCompleted();
        CompletableFuture<Object> mockedUDFThread1 = new CompletableFuture<>();
        CompletableFuture<Boolean> hasNextFinishIdentifier1 = new CompletableFuture<>();
        mockedUDFThread1.thenRunAsync(
                () -> {
                    boolean hasNext = iterator.hasNext();
                    hasNextFinishIdentifier1.complete(hasNext);
                });
        mockedUDFThread1.complete(null);
        assertThat(hasNextFinishIdentifier1).isNotCompleted();
        iterator.addRecord(RECORD);
        hasNextFinishIdentifier1.get();
        assertThat(hasNextFinishIdentifier1).isCompletedWithValue(true);
        iterator.next();
        // 2.Test hasNext() when the MapPartitionIterator is closed.
        CompletableFuture<Object> mockedUDFThread2 = new CompletableFuture<>();
        CompletableFuture<Boolean> hasNextFinishIdentifier2 = new CompletableFuture<>();
        mockedUDFThread2.thenRunAsync(
                () -> {
                    boolean hasNext = iterator.hasNext();
                    hasNextFinishIdentifier2.complete(hasNext);
                    udfFinishTrigger.complete(null);
                });
        mockedUDFThread2.complete(null);
        assertThat(hasNextFinishIdentifier2).isNotCompleted();
        iterator.close();
        assertThat(hasNextFinishIdentifier2).isCompletedWithValue(false);
        assertThat(udfFinishTrigger).isCompleted();
    }

    @Test
    void testNext() throws ExecutionException, InterruptedException {
        CompletableFuture<List<String>> result = new CompletableFuture<>();
        CompletableFuture<Object> udfFinishTrigger = new CompletableFuture<>();
        MapPartitionIterator<String> iterator =
                new MapPartitionIterator<>(
                        inputIterator -> {
                            List<String> strings = new ArrayList<>();
                            for (int index = 0; index < RECORD_NUMBER; ++index) {
                                strings.add(inputIterator.next());
                            }
                            result.complete(strings);
                            try {
                                udfFinishTrigger.get();
                            } catch (InterruptedException | ExecutionException e) {
                                ExceptionUtils.rethrow(e);
                            }
                        });
        // 1.Test next() when the cache is not empty in the MapPartitionIterator.
        addRecordToIterator(RECORD_NUMBER, iterator);
        List<String> results = result.get();
        assertThat(results.size()).isEqualTo(RECORD_NUMBER);
        assertThat(results.get(0)).isEqualTo(RECORD);
        assertThat(results.get(1)).isEqualTo(RECORD);
        assertThat(results.get(2)).isEqualTo(RECORD);
        // 2.Test next() when the cache is empty in the MapPartitionIterator.
        CompletableFuture<Object> mockedUDFThread1 = new CompletableFuture<>();
        CompletableFuture<String> nextFinishIdentifier1 = new CompletableFuture<>();
        mockedUDFThread1.thenRunAsync(
                () -> {
                    String next = iterator.next();
                    nextFinishIdentifier1.complete(next);
                });
        mockedUDFThread1.complete(null);
        assertThat(nextFinishIdentifier1).isNotCompleted();
        iterator.addRecord(RECORD);
        nextFinishIdentifier1.get();
        assertThat(nextFinishIdentifier1).isCompletedWithValue(RECORD);
        // 2.Test next() when the MapPartitionIterator is closed.
        CompletableFuture<Object> mockedUDFThread2 = new CompletableFuture<>();
        CompletableFuture<String> nextFinishIdentifier2 = new CompletableFuture<>();
        mockedUDFThread2.thenRunAsync(
                () -> {
                    String next = iterator.next();
                    nextFinishIdentifier2.complete(next);
                    udfFinishTrigger.complete(null);
                });
        mockedUDFThread2.complete(null);
        assertThat(nextFinishIdentifier2).isNotCompleted();
        iterator.close();
        assertThat(nextFinishIdentifier2).isCompletedWithValue(null);
        assertThat(udfFinishTrigger).isCompleted();
    }

    @Test
    void testClose() throws ExecutionException, InterruptedException {
        // 1.Test close() when the cache is not empty in the MapPartitionIterator.
        CompletableFuture<?> udfFinishTrigger1 = new CompletableFuture<>();
        MapPartitionIterator<String> iterator1 =
                new MapPartitionIterator<>(
                        ignored -> {
                            try {
                                udfFinishTrigger1.get();
                            } catch (InterruptedException | ExecutionException e) {
                                ExceptionUtils.rethrow(e);
                            }
                        });
        iterator1.addRecord(RECORD);
        CompletableFuture<Object> mockedTaskThread1 = new CompletableFuture<>();
        CompletableFuture<Object> iteratorCloseIdentifier1 = new CompletableFuture<>();
        mockedTaskThread1.thenRunAsync(
                () -> {
                    iterator1.close();
                    iteratorCloseIdentifier1.complete(null);
                });
        mockedTaskThread1.complete(null);
        assertThat(iteratorCloseIdentifier1).isNotCompleted();
        udfFinishTrigger1.complete(null);
        iteratorCloseIdentifier1.get();
        assertThat(iteratorCloseIdentifier1).isCompleted();
        // 2.Test close() when the cache is empty in the MapPartitionIterator.
        CompletableFuture<?> udfFinishTrigger2 = new CompletableFuture<>();
        MapPartitionIterator<String> iterator2 =
                new MapPartitionIterator<>(
                        ignored -> {
                            try {
                                udfFinishTrigger2.get();
                            } catch (InterruptedException | ExecutionException e) {
                                ExceptionUtils.rethrow(e);
                            }
                        });
        CompletableFuture<Object> mockedTaskThread2 = new CompletableFuture<>();
        CompletableFuture<Object> iteratorCloseIdentifier2 = new CompletableFuture<>();
        mockedTaskThread1.thenRunAsync(
                () -> {
                    iterator2.close();
                    iteratorCloseIdentifier2.complete(null);
                });
        mockedTaskThread2.complete(null);
        assertThat(iteratorCloseIdentifier2).isNotCompleted();
        udfFinishTrigger2.complete(null);
        iteratorCloseIdentifier2.get();
        assertThat(iteratorCloseIdentifier2).isCompleted();
        // 2.Test close() when the udf is finished in the MapPartitionIterator.
        MapPartitionIterator<String> iterator3 = new MapPartitionIterator<>(ignored -> {});
        iterator3.close();
    }

    private void addRecordToIterator(int cacheNumber, MapPartitionIterator<String> iterator) {
        for (int index = 0; index < cacheNumber; ++index) {
            iterator.addRecord(RECORD);
        }
    }
}
