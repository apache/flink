/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.SerializerFactory;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.externalresource.ExternalResourceInfoProvider;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.query.KvStateRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackendParametersImpl;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.state.v2.DefaultKeyedStateStoreV2;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.streaming.util.CollectorOutput;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link StreamingRuntimeContext}. */
class StreamingRuntimeContextTest {

    @Test
    void testValueStateInstantiation() throws Exception {

        final ExecutionConfig config = new ExecutionConfig();
        ((SerializerConfigImpl) config.getSerializerConfig()).registerKryoType(Path.class);

        final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

        StreamingRuntimeContext context = createRuntimeContext(descriptorCapture, config, false);
        ValueStateDescriptor<TaskInfo> descr = new ValueStateDescriptor<>("name", TaskInfo.class);
        context.getState(descr);

        StateDescriptor<?, ?> descrIntercepted = (StateDescriptor<?, ?>) descriptorCapture.get();
        TypeSerializer<?> serializer = descrIntercepted.getSerializer();

        // check that the Path class is really registered, i.e., the execution config was applied
        assertThat(serializer).isInstanceOf(KryoSerializer.class);
        assertThat(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId())
                .isPositive();
    }

    @Test
    void testReducingStateInstantiation() throws Exception {

        final ExecutionConfig config = new ExecutionConfig();
        ((SerializerConfigImpl) config.getSerializerConfig()).registerKryoType(Path.class);

        final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

        StreamingRuntimeContext context = createRuntimeContext(descriptorCapture, config, false);

        @SuppressWarnings("unchecked")
        ReduceFunction<TaskInfo> reducer = (ReduceFunction<TaskInfo>) mock(ReduceFunction.class);

        ReducingStateDescriptor<TaskInfo> descr =
                new ReducingStateDescriptor<>("name", reducer, TaskInfo.class);

        context.getReducingState(descr);

        StateDescriptor<?, ?> descrIntercepted = (StateDescriptor<?, ?>) descriptorCapture.get();
        TypeSerializer<?> serializer = descrIntercepted.getSerializer();

        // check that the Path class is really registered, i.e., the execution config was applied
        assertThat(serializer).isInstanceOf(KryoSerializer.class);
        assertThat(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId())
                .isPositive();
    }

    @Test
    void testAggregatingStateInstantiation() throws Exception {
        final ExecutionConfig config = new ExecutionConfig();
        ((SerializerConfigImpl) config.getSerializerConfig()).registerKryoType(Path.class);

        final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

        StreamingRuntimeContext context = createRuntimeContext(descriptorCapture, config, false);

        @SuppressWarnings("unchecked")
        AggregateFunction<String, TaskInfo, String> aggregate =
                (AggregateFunction<String, TaskInfo, String>) mock(AggregateFunction.class);

        AggregatingStateDescriptor<String, TaskInfo, String> descr =
                new AggregatingStateDescriptor<>("name", aggregate, TaskInfo.class);

        context.getAggregatingState(descr);

        AggregatingStateDescriptor<?, ?, ?> descrIntercepted =
                (AggregatingStateDescriptor<?, ?, ?>) descriptorCapture.get();
        TypeSerializer<?> serializer = descrIntercepted.getSerializer();

        // check that the Path class is really registered, i.e., the execution config was applied
        assertThat(serializer).isInstanceOf(KryoSerializer.class);
        assertThat(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId())
                .isPositive();
    }

    @Test
    void testListStateInstantiation() throws Exception {

        final ExecutionConfig config = new ExecutionConfig();
        ((SerializerConfigImpl) config.getSerializerConfig()).registerKryoType(Path.class);

        final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

        StreamingRuntimeContext context = createRuntimeContext(descriptorCapture, config, false);

        ListStateDescriptor<TaskInfo> descr = new ListStateDescriptor<>("name", TaskInfo.class);
        context.getListState(descr);

        ListStateDescriptor<?> descrIntercepted = (ListStateDescriptor<?>) descriptorCapture.get();
        TypeSerializer<?> serializer = descrIntercepted.getSerializer();

        // check that the Path class is really registered, i.e., the execution config was applied
        assertThat(serializer).isInstanceOf(ListSerializer.class);

        TypeSerializer<?> elementSerializer = descrIntercepted.getElementSerializer();
        assertThat(elementSerializer).isInstanceOf(KryoSerializer.class);
        assertThat(
                        ((KryoSerializer<?>) elementSerializer)
                                .getKryo()
                                .getRegistration(Path.class)
                                .getId())
                .isPositive();
    }

    @Test
    void testListStateReturnsEmptyListByDefault() throws Exception {
        StreamingRuntimeContext context = createRuntimeContext();

        ListStateDescriptor<String> descr = new ListStateDescriptor<>("name", String.class);
        ListState<String> state = context.getListState(descr);

        Iterable<String> value = state.get();
        assertThat(value).isNotNull();
        assertThat(value.iterator()).isExhausted();
    }

    @Test
    void testMapStateInstantiation() throws Exception {

        final ExecutionConfig config = new ExecutionConfig();
        ((SerializerConfigImpl) config.getSerializerConfig()).registerKryoType(Path.class);

        final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

        StreamingRuntimeContext context = createRuntimeContext(descriptorCapture, config, false);

        MapStateDescriptor<String, TaskInfo> descr =
                new MapStateDescriptor<>("name", String.class, TaskInfo.class);

        context.getMapState(descr);

        MapStateDescriptor<?, ?> descrIntercepted =
                (MapStateDescriptor<?, ?>) descriptorCapture.get();
        TypeSerializer<?> valueSerializer = descrIntercepted.getValueSerializer();

        // check that the Path class is really registered, i.e., the execution config was applied
        assertThat(valueSerializer).isInstanceOf(KryoSerializer.class);
        assertThat(
                        ((KryoSerializer<?>) valueSerializer)
                                .getKryo()
                                .getRegistration(Path.class)
                                .getId())
                .isPositive();
    }

    @Test
    void testMapStateReturnsEmptyMapByDefault() throws Exception {

        StreamingRuntimeContext context = createMapOperatorRuntimeContext();

        MapStateDescriptor<Integer, String> descr =
                new MapStateDescriptor<>("name", Integer.class, String.class);
        MapState<Integer, String> state = context.getMapState(descr);

        Iterable<Map.Entry<Integer, String>> value = state.entries();
        assertThat(value).isNotNull();
        assertThat(value.iterator()).isExhausted();
    }

    @Test
    void testV2ValueStateInstantiation() throws Exception {

        final ExecutionConfig config = new ExecutionConfig();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) config.getSerializerConfig();
        serializerConfig.registerKryoType(Path.class);

        final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

        StreamingRuntimeContext context = createRuntimeContext(descriptorCapture, config, true);
        org.apache.flink.runtime.state.v2.ValueStateDescriptor<TaskInfo> descr =
                new org.apache.flink.runtime.state.v2.ValueStateDescriptor<>(
                        "name", TypeInformation.of(TaskInfo.class), serializerConfig);
        context.getValueState(descr);

        org.apache.flink.runtime.state.v2.ValueStateDescriptor<?> descrIntercepted =
                (org.apache.flink.runtime.state.v2.ValueStateDescriptor<?>) descriptorCapture.get();
        TypeSerializer<?> serializer = descrIntercepted.getSerializer();

        // check that the Path class is really registered, i.e., the execution config was applied
        assertThat(serializer).isInstanceOf(KryoSerializer.class);
        assertThat(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId())
                .isPositive();
    }

    @Test
    void testV2ListStateInstantiation() throws Exception {
        final ExecutionConfig config = new ExecutionConfig();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) config.getSerializerConfig();
        serializerConfig.registerKryoType(Path.class);

        final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

        StreamingRuntimeContext context = createRuntimeContext(descriptorCapture, config, true);
        org.apache.flink.runtime.state.v2.ListStateDescriptor<TaskInfo> descr =
                new org.apache.flink.runtime.state.v2.ListStateDescriptor<>(
                        "name", TypeInformation.of(TaskInfo.class), serializerConfig);
        context.getListState(descr);

        org.apache.flink.runtime.state.v2.ListStateDescriptor<?> descrIntercepted =
                (org.apache.flink.runtime.state.v2.ListStateDescriptor<?>) descriptorCapture.get();
        TypeSerializer<?> serializer = descrIntercepted.getSerializer();

        // check that the Path class is really registered, i.e., the execution config was applied
        assertThat(serializer).isInstanceOf(KryoSerializer.class);
        assertThat(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId())
                .isPositive();
    }

    @Test
    void testV2MapStateInstantiation() throws Exception {
        final ExecutionConfig config = new ExecutionConfig();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) config.getSerializerConfig();
        serializerConfig.registerKryoType(Path.class);

        final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

        StreamingRuntimeContext context = createRuntimeContext(descriptorCapture, config, true);
        org.apache.flink.runtime.state.v2.MapStateDescriptor<String, TaskInfo> descr =
                new org.apache.flink.runtime.state.v2.MapStateDescriptor<>(
                        "name",
                        TypeInformation.of(String.class),
                        TypeInformation.of(TaskInfo.class),
                        serializerConfig);
        context.getMapState(descr);

        org.apache.flink.runtime.state.v2.MapStateDescriptor<?, ?> descrIntercepted =
                (org.apache.flink.runtime.state.v2.MapStateDescriptor<?, ?>)
                        descriptorCapture.get();
        TypeSerializer<?> serializer = descrIntercepted.getSerializer();

        // check that the Path class is really registered, i.e., the execution config was applied
        assertThat(serializer).isInstanceOf(KryoSerializer.class);
        assertThat(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId())
                .isPositive();
    }

    @Test
    void testV2ReducingStateInstantiation() throws Exception {
        final ExecutionConfig config = new ExecutionConfig();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) config.getSerializerConfig();
        serializerConfig.registerKryoType(Path.class);

        final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

        StreamingRuntimeContext context = createRuntimeContext(descriptorCapture, config, true);

        @SuppressWarnings("unchecked")
        ReduceFunction<TaskInfo> reducer = (ReduceFunction<TaskInfo>) mock(ReduceFunction.class);

        org.apache.flink.runtime.state.v2.ReducingStateDescriptor<TaskInfo> descr =
                new org.apache.flink.runtime.state.v2.ReducingStateDescriptor<>(
                        "name", reducer, TypeInformation.of(TaskInfo.class), serializerConfig);

        context.getReducingState(descr);

        org.apache.flink.runtime.state.v2.ReducingStateDescriptor<?> descrIntercepted =
                (org.apache.flink.runtime.state.v2.ReducingStateDescriptor<?>)
                        descriptorCapture.get();
        TypeSerializer<?> serializer = descrIntercepted.getSerializer();

        // check that the Path class is really registered, i.e., the execution config was applied
        assertThat(serializer).isInstanceOf(KryoSerializer.class);
        assertThat(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId())
                .isPositive();
    }

    @Test
    void testV2AggregatingStateInstantiation() throws Exception {
        final ExecutionConfig config = new ExecutionConfig();
        SerializerConfigImpl serializerConfig = (SerializerConfigImpl) config.getSerializerConfig();
        serializerConfig.registerKryoType(Path.class);

        final AtomicReference<Object> descriptorCapture = new AtomicReference<>();

        StreamingRuntimeContext context = createRuntimeContext(descriptorCapture, config, true);

        @SuppressWarnings("unchecked")
        AggregateFunction<String, TaskInfo, String> aggregate =
                (AggregateFunction<String, TaskInfo, String>) mock(AggregateFunction.class);

        org.apache.flink.runtime.state.v2.AggregatingStateDescriptor<String, TaskInfo, String>
                descr =
                        new org.apache.flink.runtime.state.v2.AggregatingStateDescriptor<>(
                                "name",
                                aggregate,
                                TypeInformation.of(TaskInfo.class),
                                serializerConfig);

        context.getAggregatingState(descr);

        org.apache.flink.runtime.state.v2.AggregatingStateDescriptor<?, ?, ?> descrIntercepted =
                (org.apache.flink.runtime.state.v2.AggregatingStateDescriptor<?, ?, ?>)
                        descriptorCapture.get();
        TypeSerializer<?> serializer = descrIntercepted.getSerializer();

        // check that the Path class is really registered, i.e., the execution config was applied
        assertThat(serializer).isInstanceOf(KryoSerializer.class);
        assertThat(((KryoSerializer<?>) serializer).getKryo().getRegistration(Path.class).getId())
                .isPositive();
    }

    // ------------------------------------------------------------------------
    //
    // ------------------------------------------------------------------------

    private StreamingRuntimeContext createMapOperatorRuntimeContext() throws Exception {
        AbstractStreamOperator<?> mapPlainMockOp = createMapPlainMockOp();
        return createRuntimeContext(mapPlainMockOp);
    }

    private StreamingRuntimeContext createRuntimeContext() throws Exception {
        return new StreamingRuntimeContext(
                createListPlainMockOp(), MockEnvironment.builder().build(), Collections.emptyMap());
    }

    private StreamingRuntimeContext createRuntimeContext(
            AtomicReference<Object> descriptorCapture, ExecutionConfig config, boolean stateV2)
            throws Exception {
        return createDescriptorCapturingMockOp(
                        descriptorCapture,
                        config,
                        MockEnvironment.builder().setExecutionConfig(config).build(),
                        stateV2)
                .getRuntimeContext();
    }

    private StreamingRuntimeContext createRuntimeContext(AbstractStreamOperator<?> operator) {
        return new StreamingRuntimeContext(
                MockEnvironment.builder().build(),
                Collections.emptyMap(),
                operator.getMetricGroup(),
                operator.getOperatorID(),
                operator.getProcessingTimeService(),
                operator.getKeyedStateStore(),
                ExternalResourceInfoProvider.NO_EXTERNAL_RESOURCES);
    }

    @SuppressWarnings("unchecked")
    private static AbstractStreamOperator<?> createDescriptorCapturingMockOp(
            final AtomicReference<Object> ref,
            final ExecutionConfig config,
            Environment environment,
            boolean stateV2)
            throws Exception {

        StreamConfig streamConfig = new StreamConfig(new Configuration());
        streamConfig.setOperatorID(new OperatorID());
        AbstractStreamOperator<?> operator =
                new AbstractStreamOperator<Object>(
                        new StreamOperatorParameters<>(
                                new MockStreamTaskBuilder(environment)
                                        .setExecutionConfig(config)
                                        .build(),
                                streamConfig,
                                new CollectorOutput<>(new ArrayList<>()),
                                TestProcessingTimeService::new,
                                null,
                                null)) {
                    @Override
                    protected void setup(
                            StreamTask<?, ?> containingTask,
                            StreamConfig config,
                            Output<StreamRecord<Object>> output) {
                        super.setup(containingTask, config, output);
                    }
                };

        StreamTaskStateInitializer streamTaskStateManager =
                new StreamTaskStateInitializerImpl(environment, new HashMapStateBackend());

        KeyedStateBackend keyedStateBackend = mock(KeyedStateBackend.class);

        AsyncKeyedStateBackend asyncKeyedStateBackend = mock(AsyncKeyedStateBackend.class);

        DefaultKeyedStateStore keyedStateStore =
                new DefaultKeyedStateStore(
                        keyedStateBackend,
                        new SerializerFactory() {
                            @Override
                            public <T> TypeSerializer<T> createSerializer(
                                    TypeInformation<T> typeInformation) {
                                return typeInformation.createSerializer(
                                        config.getSerializerConfig());
                            }
                        });

        doAnswer(
                        (Answer<Object>)
                                invocationOnMock -> {
                                    ref.set(invocationOnMock.getArguments()[2]);
                                    return null;
                                })
                .when(keyedStateBackend)
                .getPartitionedState(
                        ArgumentMatchers.any(),
                        any(TypeSerializer.class),
                        any(StateDescriptor.class));

        doAnswer(
                        (Answer<Object>)
                                invocationOnMock -> {
                                    ref.set(invocationOnMock.getArguments()[2]);
                                    return null;
                                })
                .when(asyncKeyedStateBackend)
                .createState(
                        any(),
                        any(TypeSerializer.class),
                        any(org.apache.flink.runtime.state.v2.StateDescriptor.class));

        operator.initializeState(streamTaskStateManager);
        if (!stateV2) {
            operator.getRuntimeContext().setKeyedStateStore(keyedStateStore);
        } else {
            operator.getRuntimeContext()
                    .setKeyedStateStoreV2(new DefaultKeyedStateStoreV2(asyncKeyedStateBackend));
        }

        return operator;
    }

    @SuppressWarnings("unchecked")
    private static AbstractStreamOperator<?> createListPlainMockOp() throws Exception {

        AbstractStreamOperator<?> operatorMock = mock(AbstractStreamOperator.class);
        ExecutionConfig config = new ExecutionConfig();

        KeyedStateBackend keyedStateBackend = mock(KeyedStateBackend.class);

        DefaultKeyedStateStore keyedStateStore =
                new DefaultKeyedStateStore(
                        keyedStateBackend,
                        new SerializerFactory() {
                            @Override
                            public <T> TypeSerializer<T> createSerializer(
                                    TypeInformation<T> typeInformation) {
                                return typeInformation.createSerializer(
                                        config.getSerializerConfig());
                            }
                        });

        when(operatorMock.getExecutionConfig()).thenReturn(config);

        doAnswer(
                        new Answer<ListState<String>>() {

                            @Override
                            public ListState<String> answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                ListStateDescriptor<String> descr =
                                        (ListStateDescriptor<String>)
                                                invocationOnMock.getArguments()[2];

                                AbstractStateBackend abstractStateBackend =
                                        new HashMapStateBackend();
                                Environment env = new DummyEnvironment("test_task", 1, 0);
                                JobID jobID = new JobID();
                                KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
                                TaskKvStateRegistry kvStateRegistry =
                                        new KvStateRegistry()
                                                .createTaskRegistry(new JobID(), new JobVertexID());
                                CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
                                AbstractKeyedStateBackend<Integer> backend =
                                        abstractStateBackend.createKeyedStateBackend(
                                                new KeyedStateBackendParametersImpl<>(
                                                        env,
                                                        jobID,
                                                        "test_op",
                                                        IntSerializer.INSTANCE,
                                                        1,
                                                        keyGroupRange,
                                                        kvStateRegistry,
                                                        TtlTimeProvider.DEFAULT,
                                                        new UnregisteredMetricsGroup(),
                                                        Collections.emptyList(),
                                                        cancelStreamRegistry));
                                backend.setCurrentKey(0);
                                return backend.getPartitionedState(
                                        VoidNamespace.INSTANCE,
                                        VoidNamespaceSerializer.INSTANCE,
                                        descr);
                            }
                        })
                .when(keyedStateBackend)
                .getPartitionedState(
                        ArgumentMatchers.any(),
                        any(TypeSerializer.class),
                        any(ListStateDescriptor.class));

        when(operatorMock.getKeyedStateStore()).thenReturn(keyedStateStore);
        when(operatorMock.getOperatorID()).thenReturn(new OperatorID());
        return operatorMock;
    }

    @SuppressWarnings("unchecked")
    private static AbstractStreamOperator<?> createMapPlainMockOp() throws Exception {

        AbstractStreamOperator<?> operatorMock = mock(AbstractStreamOperator.class);
        ExecutionConfig config = new ExecutionConfig();

        KeyedStateBackend keyedStateBackend = mock(KeyedStateBackend.class);

        DefaultKeyedStateStore keyedStateStore =
                new DefaultKeyedStateStore(
                        keyedStateBackend,
                        new SerializerFactory() {
                            @Override
                            public <T> TypeSerializer<T> createSerializer(
                                    TypeInformation<T> typeInformation) {
                                return typeInformation.createSerializer(
                                        config.getSerializerConfig());
                            }
                        });

        when(operatorMock.getExecutionConfig()).thenReturn(config);

        doAnswer(
                        new Answer<MapState<Integer, String>>() {

                            @Override
                            public MapState<Integer, String> answer(
                                    InvocationOnMock invocationOnMock) throws Throwable {
                                MapStateDescriptor<Integer, String> descr =
                                        (MapStateDescriptor<Integer, String>)
                                                invocationOnMock.getArguments()[2];

                                AbstractStateBackend abstractStateBackend =
                                        new HashMapStateBackend();
                                Environment env = new DummyEnvironment("test_task", 1, 0);
                                JobID jobID = new JobID();
                                KeyGroupRange keyGroupRange = new KeyGroupRange(0, 0);
                                TaskKvStateRegistry kvStateRegistry =
                                        new KvStateRegistry()
                                                .createTaskRegistry(new JobID(), new JobVertexID());
                                CloseableRegistry cancelStreamRegistry = new CloseableRegistry();
                                AbstractKeyedStateBackend<Integer> backend =
                                        abstractStateBackend.createKeyedStateBackend(
                                                new KeyedStateBackendParametersImpl<>(
                                                        env,
                                                        jobID,
                                                        "test_op",
                                                        IntSerializer.INSTANCE,
                                                        1,
                                                        keyGroupRange,
                                                        kvStateRegistry,
                                                        TtlTimeProvider.DEFAULT,
                                                        new UnregisteredMetricsGroup(),
                                                        Collections.emptyList(),
                                                        cancelStreamRegistry));
                                backend.setCurrentKey(0);
                                return backend.getPartitionedState(
                                        VoidNamespace.INSTANCE,
                                        VoidNamespaceSerializer.INSTANCE,
                                        descr);
                            }
                        })
                .when(keyedStateBackend)
                .getPartitionedState(
                        ArgumentMatchers.any(),
                        any(TypeSerializer.class),
                        any(MapStateDescriptor.class));

        when(operatorMock.getKeyedStateStore()).thenReturn(keyedStateStore);
        when(operatorMock.getOperatorID()).thenReturn(new OperatorID());
        when(operatorMock.getProcessingTimeService()).thenReturn(new TestProcessingTimeService());
        return operatorMock;
    }
}
