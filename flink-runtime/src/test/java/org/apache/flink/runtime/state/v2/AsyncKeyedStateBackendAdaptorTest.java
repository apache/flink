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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.v2.adaptor.AsyncKeyedStateBackendAdaptor;
import org.apache.flink.runtime.state.v2.internal.InternalAggregatingState;
import org.apache.flink.runtime.state.v2.internal.InternalReducingState;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AsyncKeyedStateBackendAdaptor}. */
public class AsyncKeyedStateBackendAdaptorTest {

    @Test
    public void testValueStateAdaptor() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedStateBackend = createBackend();
        AsyncKeyedStateBackendAdaptor<String> adaptor =
                new AsyncKeyedStateBackendAdaptor<>(keyedStateBackend);
        keyedStateBackend.setCurrentKey("test");
        StateDescriptor<Integer> descriptor =
                new ValueStateDescriptor<>("testState", BasicTypeInfo.INT_TYPE_INFO);

        org.apache.flink.api.common.state.v2.ValueState<Integer> valueState =
                adaptor.createState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

        // test synchronous interfaces.
        valueState.clear();
        assertThat(valueState.value()).isNull();
        valueState.update(10);
        assertThat(valueState.value()).isEqualTo(10);

        // test asynchronous interfaces.
        valueState
                .asyncClear()
                .thenCompose(
                        clear -> {
                            assertThat(valueState.value()).isNull();
                            return valueState.asyncUpdate(20);
                        })
                .thenCompose(
                        empty -> {
                            assertThat(valueState.value()).isEqualTo(20);
                            return valueState.asyncValue();
                        })
                .thenAccept(
                        value -> {
                            assertThat(value).isEqualTo(20);
                        });
        adaptor.close();
    }

    @Test
    public void testListStateAdaptor() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedStateBackend = createBackend();
        AsyncKeyedStateBackendAdaptor<String> adaptor =
                new AsyncKeyedStateBackendAdaptor<>(keyedStateBackend);
        keyedStateBackend.setCurrentKey("test");
        StateDescriptor<Integer> descriptor =
                new ListStateDescriptor<>("testState", BasicTypeInfo.INT_TYPE_INFO);

        org.apache.flink.api.common.state.v2.ListState<Integer> listState =
                adaptor.createState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

        // test synchronous interfaces.
        listState.clear();
        assertThat(listState.get()).isNull();
        listState.add(10);
        assertThat(listState.get()).containsExactlyInAnyOrderElementsOf(Arrays.asList(10));
        listState.addAll(Arrays.asList(20, 30));
        assertThat(listState.get()).containsExactlyInAnyOrderElementsOf(Arrays.asList(10, 20, 30));
        listState.update(Arrays.asList(40, 50));
        assertThat(listState.get()).containsExactlyInAnyOrderElementsOf(Arrays.asList(40, 50));

        // test asynchronous interfaces.
        listState
                .asyncClear()
                .thenCompose(
                        clear -> {
                            assertThat(listState.get()).isNull();
                            return listState.asyncAdd(10);
                        })
                .thenCompose(
                        empty -> {
                            assertThat(listState.get())
                                    .containsExactlyInAnyOrderElementsOf(Arrays.asList(10));
                            return listState.asyncAddAll(Arrays.asList(20, 30));
                        })
                .thenCompose(
                        empty -> {
                            assertThat(listState.get())
                                    .containsExactlyInAnyOrderElementsOf(Arrays.asList(10, 20, 30));
                            return listState.asyncUpdate(Arrays.asList(40, 50));
                        })
                .thenAccept(
                        empty -> {
                            assertThat(listState.get())
                                    .containsExactlyInAnyOrderElementsOf(Arrays.asList(40, 50));
                        });
        adaptor.close();
    }

    @Test
    public void testMapStateAdaptor() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedStateBackend = createBackend();
        AsyncKeyedStateBackendAdaptor<String> adaptor =
                new AsyncKeyedStateBackendAdaptor<>(keyedStateBackend);
        keyedStateBackend.setCurrentKey("test");
        StateDescriptor<Integer> descriptor =
                new MapStateDescriptor<>(
                        "testState", BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

        org.apache.flink.api.common.state.v2.MapState<Integer, Integer> mapState =
                adaptor.createState(
                        VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, descriptor);

        final HashMap<Integer, Integer> groundTruth =
                new HashMap<Integer, Integer>() {
                    {
                        put(10, 100);
                        put(20, 200);
                    }
                };

        // test synchronous interfaces.
        mapState.clear();
        assertThat(mapState.isEmpty()).isTrue();
        mapState.put(10, 100);
        assertThat(mapState.get(10)).isEqualTo(100);
        mapState.putAll(
                new HashMap<Integer, Integer>() {
                    {
                        put(20, 200);
                    }
                });
        assertThat(mapState.get(20)).isEqualTo(200);
        assertThat(mapState.contains(20)).isTrue();
        assertThat(mapState.entries()).containsExactlyInAnyOrderElementsOf(groundTruth.entrySet());
        assertThat(mapState.keys()).containsExactlyInAnyOrderElementsOf(groundTruth.keySet());
        assertThat(mapState.values()).containsExactlyInAnyOrderElementsOf(groundTruth.values());
        assertThat(mapState.iterator())
                .toIterable()
                .containsExactlyInAnyOrderElementsOf(groundTruth.entrySet());

        // test asynchronous interfaces.
        mapState.asyncClear()
                .thenCompose(
                        clear -> {
                            assertThat(mapState.isEmpty()).isTrue();
                            return mapState.asyncPut(10, 100);
                        })
                .thenCompose(
                        empty -> {
                            assertThat(mapState.get(10)).isEqualTo(100);
                            return mapState.asyncPutAll(groundTruth);
                        })
                .thenCompose(
                        empty -> {
                            assertThat(mapState.get(20)).isEqualTo(200);
                            return mapState.asyncContains(20);
                        })
                .thenCompose(
                        contains -> {
                            assertThat(contains).isTrue();
                            return mapState.asyncEntries();
                        })
                .thenCompose(
                        iterator -> {
                            HashMap<Integer, Integer> iterated = new HashMap<>();
                            iterator.onNext(
                                    entry -> {
                                        iterated.put(entry.getKey(), entry.getValue());
                                    });
                            assertThat(iterated.entrySet())
                                    .containsExactlyInAnyOrderElementsOf(groundTruth.entrySet());
                            return mapState.asyncKeys();
                        })
                .thenCompose(
                        iterator -> {
                            HashSet<Integer> iterated = new HashSet<>();
                            iterator.onNext(iterated::add);
                            assertThat(iterated)
                                    .containsExactlyInAnyOrderElementsOf(groundTruth.keySet());
                            return mapState.asyncValues();
                        })
                .thenAccept(
                        iterator -> {
                            HashSet<Integer> iterated = new HashSet<>();
                            iterator.onNext(iterated::add);
                            assertThat(iterated)
                                    .containsExactlyInAnyOrderElementsOf(groundTruth.values());
                        });
        adaptor.close();
    }

    @Test
    public void testReducingStateAdaptor() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedStateBackend = createBackend();
        AsyncKeyedStateBackendAdaptor<String> adaptor =
                new AsyncKeyedStateBackendAdaptor<>(keyedStateBackend);
        keyedStateBackend.setCurrentKey("test");
        StateDescriptor<Integer> descriptor =
                new ReducingStateDescriptor<>(
                        "testState", Integer::sum, BasicTypeInfo.INT_TYPE_INFO);

        InternalReducingState<String, Long, Integer> reducingState =
                adaptor.createState(0L, LongSerializer.INSTANCE, descriptor);

        // test synchronous interfaces.
        reducingState.clear();
        assertThat(reducingState.get()).isNull();
        reducingState.add(10);
        assertThat(reducingState.get()).isEqualTo(10);
        reducingState.add(20);
        assertThat(reducingState.get()).isEqualTo(30);
        reducingState.setCurrentNamespace(1L);
        assertThat(reducingState.get()).isNull();
        reducingState.add(30);
        reducingState.mergeNamespaces(0L, Arrays.asList(0L, 1L));
        reducingState.setCurrentNamespace(0L);
        assertThat(reducingState.get()).isEqualTo(60);
        reducingState.setCurrentNamespace(1L);
        assertThat(reducingState.get()).isNull();

        // test asynchronous interfaces.
        reducingState.setCurrentNamespace(0L);
        reducingState
                .asyncClear()
                .thenCompose(
                        clear -> {
                            assertThat(reducingState.get()).isNull();
                            return reducingState.asyncAdd(10);
                        })
                .thenCompose(
                        empty -> {
                            return reducingState.asyncGet();
                        })
                .thenCompose(
                        value -> {
                            assertThat(value).isEqualTo(10);
                            return reducingState.asyncAdd(20);
                        })
                .thenCompose(
                        empty -> {
                            return reducingState.asyncGet();
                        })
                .thenCompose(
                        value -> {
                            assertThat(value).isEqualTo(30);
                            reducingState.setCurrentNamespace(1L);
                            return reducingState.asyncGet();
                        })
                .thenCompose(
                        value -> {
                            assertThat(value).isNull();
                            return reducingState.asyncAdd(30);
                        })
                .thenCompose(
                        empty -> {
                            return reducingState.asyncMergeNamespaces(0L, Arrays.asList(0L, 1L));
                        })
                .thenCompose(
                        empty -> {
                            reducingState.setCurrentNamespace(0L);
                            return reducingState.asyncGet();
                        })
                .thenCompose(
                        value -> {
                            assertThat(value).isEqualTo(60);
                            reducingState.setCurrentNamespace(1L);
                            return reducingState.asyncGet();
                        })
                .thenAccept(
                        value -> {
                            assertThat(value).isNull();
                        });
        adaptor.close();
    }

    @Test
    public void testAggregatingStateAdaptor() throws Exception {
        CheckpointableKeyedStateBackend<String> keyedStateBackend = createBackend();
        AsyncKeyedStateBackendAdaptor<String> adaptor =
                new AsyncKeyedStateBackendAdaptor<>(keyedStateBackend);
        keyedStateBackend.setCurrentKey("test");
        StateDescriptor<Integer> descriptor =
                new AggregatingStateDescriptor<>(
                        "testState",
                        new AggregateFunction<Integer, Integer, String>() {
                            @Override
                            public Integer createAccumulator() {
                                return 0;
                            }

                            @Override
                            public Integer add(Integer value, Integer accumulator) {
                                return accumulator + value;
                            }

                            @Override
                            public String getResult(Integer accumulator) {
                                return accumulator.toString();
                            }

                            @Override
                            public Integer merge(Integer a, Integer b) {
                                return a + b;
                            }
                        },
                        BasicTypeInfo.INT_TYPE_INFO);

        InternalAggregatingState<String, Long, Integer, Integer, String> aggState =
                adaptor.createState(0L, LongSerializer.INSTANCE, descriptor);

        // test synchronous interfaces.
        aggState.clear();
        assertThat(aggState.get()).isNull();
        aggState.add(10);
        assertThat(aggState.get()).isEqualTo("10");
        aggState.add(20);
        assertThat(aggState.get()).isEqualTo("30");
        aggState.setCurrentNamespace(1L);
        assertThat(aggState.get()).isNull();
        aggState.add(30);
        aggState.mergeNamespaces(0L, Arrays.asList(0L, 1L));
        aggState.setCurrentNamespace(0L);
        assertThat(aggState.get()).isEqualTo("60");
        aggState.setCurrentNamespace(1L);
        assertThat(aggState.get()).isNull();

        // test asynchronous interfaces.
        aggState.setCurrentNamespace(0L);
        aggState.asyncClear()
                .thenCompose(
                        clear -> {
                            assertThat(aggState.get()).isNull();
                            return aggState.asyncAdd(10);
                        })
                .thenCompose(
                        empty -> {
                            return aggState.asyncGet();
                        })
                .thenCompose(
                        value -> {
                            assertThat(value).isEqualTo("10");
                            return aggState.asyncAdd(20);
                        })
                .thenCompose(
                        empty -> {
                            return aggState.asyncGet();
                        })
                .thenCompose(
                        value -> {
                            assertThat(value).isEqualTo("30");
                            aggState.setCurrentNamespace(1L);
                            return aggState.asyncGet();
                        })
                .thenCompose(
                        value -> {
                            assertThat(value).isNull();
                            return aggState.asyncAdd(30);
                        })
                .thenCompose(
                        empty -> {
                            return aggState.asyncMergeNamespaces(0L, Arrays.asList(0L, 1L));
                        })
                .thenCompose(
                        empty -> {
                            aggState.setCurrentNamespace(0L);
                            return aggState.asyncGet();
                        })
                .thenCompose(
                        value -> {
                            assertThat(value).isEqualTo("60");
                            aggState.setCurrentNamespace(1L);
                            return aggState.asyncGet();
                        })
                .thenAccept(
                        value -> {
                            assertThat(value).isNull();
                        });
        adaptor.close();
    }

    private static CheckpointableKeyedStateBackend<String> createBackend() throws Exception {
        ExecutionConfig executionConfig = new ExecutionConfig();
        return new HeapKeyedStateBackendBuilder<String>(
                        new KvStateRegistry().createTaskRegistry(new JobID(), new JobVertexID()),
                        StringSerializer.INSTANCE,
                        ClassLoader.getSystemClassLoader(),
                        128,
                        new KeyGroupRange(0, 127),
                        executionConfig,
                        TtlTimeProvider.DEFAULT,
                        LatencyTrackingStateConfig.disabled(),
                        Collections.EMPTY_LIST,
                        AbstractStateBackend.getCompressionDecorator(executionConfig),
                        TestLocalRecoveryConfig.disabled(),
                        new HeapPriorityQueueSetFactory(new KeyGroupRange(0, 127), 128, 128),
                        true,
                        new CloseableRegistry())
                .build();
    }
}
