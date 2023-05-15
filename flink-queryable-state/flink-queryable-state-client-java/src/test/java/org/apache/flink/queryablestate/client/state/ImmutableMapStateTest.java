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

package org.apache.flink.queryablestate.client.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.queryablestate.client.state.serialization.KvStateSerializer;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the {@link ImmutableMapState}. */
class ImmutableMapStateTest {

    private final MapStateDescriptor<Long, Long> mapStateDesc =
            new MapStateDescriptor<>(
                    "test", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);

    private MapState<Long, Long> mapState;

    @BeforeEach
    void setUp() throws Exception {
        if (!mapStateDesc.isSerializerInitialized()) {
            mapStateDesc.initializeSerializerUnlessSet(new ExecutionConfig());
        }

        Map<Long, Long> initMap = new HashMap<>();
        initMap.put(1L, 5L);
        initMap.put(2L, 5L);

        byte[] initSer =
                KvStateSerializer.serializeMap(
                        initMap.entrySet(),
                        BasicTypeInfo.LONG_TYPE_INFO.createSerializer(new ExecutionConfig()),
                        BasicTypeInfo.LONG_TYPE_INFO.createSerializer(new ExecutionConfig()));

        mapState = ImmutableMapState.createState(mapStateDesc, initSer);
    }

    @Test
    void testPut() throws Exception {
        assertThat(mapState.contains(1L)).isTrue();
        long value = mapState.get(1L);
        assertThat(value).isEqualTo(5L);

        assertThat(mapState.contains(2L)).isTrue();
        value = mapState.get(2L);
        assertThat(value).isEqualTo(5L);
        assertThatThrownBy(() -> mapState.put(2L, 54L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testPutAll() throws Exception {
        assertThat(mapState.contains(1L)).isTrue();
        long value = mapState.get(1L);
        assertThat(value).isEqualTo(5L);

        assertThat(mapState.contains(2L)).isTrue();
        value = mapState.get(2L);
        assertThat(value).isEqualTo(5L);

        Map<Long, Long> nMap = new HashMap<>();
        nMap.put(1L, 7L);
        nMap.put(2L, 7L);
        assertThatThrownBy(() -> mapState.putAll(nMap))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testUpdate() throws Exception {
        assertThat(mapState.contains(1L)).isTrue();
        long value = mapState.get(1L);
        assertThat(value).isEqualTo(5L);

        assertThat(mapState.contains(2L)).isTrue();
        value = mapState.get(2L);
        assertThat(value).isEqualTo(5L);
        assertThatThrownBy(() -> mapState.put(2L, 54L))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testIterator() throws Exception {
        assertThat(mapState.contains(1L)).isTrue();
        long value = mapState.get(1L);
        assertThat(value).isEqualTo(5L);

        assertThat(mapState.contains(2L)).isTrue();
        value = mapState.get(2L);
        assertThat(value).isEqualTo(5L);
        assertThatThrownBy(
                        () -> {
                            Iterator<Map.Entry<Long, Long>> iterator = mapState.iterator();
                            while (iterator.hasNext()) {
                                iterator.remove();
                            }
                        })
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testIterable() throws Exception {
        assertThat(mapState.contains(1L)).isTrue();
        long value = mapState.get(1L);
        assertThat(value).isEqualTo(5L);

        assertThat(mapState.contains(2L)).isTrue();
        value = mapState.get(2L);
        assertThat(value).isEqualTo(5L);
        assertThatThrownBy(
                        () -> {
                            Iterable<Map.Entry<Long, Long>> iterable = mapState.entries();
                            Iterator<Map.Entry<Long, Long>> iterator = iterable.iterator();
                            while (iterator.hasNext()) {
                                assertThat((long) iterator.next().getValue()).isEqualTo(5L);
                                iterator.remove();
                            }
                        })
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testKeys() throws Exception {
        assertThat(mapState.contains(1L)).isTrue();
        long value = mapState.get(1L);
        assertThat(value).isEqualTo(5L);

        assertThat(mapState.contains(2L)).isTrue();
        value = mapState.get(2L);
        assertThat(value).isEqualTo(5L);
        assertThatThrownBy(
                        () -> {
                            Iterator<Long> iterator = mapState.keys().iterator();
                            while (iterator.hasNext()) {
                                iterator.remove();
                            }
                        })
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testValues() throws Exception {
        assertThat(mapState.contains(1L)).isTrue();
        long value = mapState.get(1L);
        assertThat(value).isEqualTo(5L);

        assertThat(mapState.contains(2L)).isTrue();
        value = mapState.get(2L);
        assertThat(value).isEqualTo(5L);

        assertThatThrownBy(
                        () -> {
                            Iterator<Long> iterator = mapState.values().iterator();
                            while (iterator.hasNext()) {
                                iterator.remove();
                            }
                        })
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testClear() throws Exception {
        assertThat(mapState.contains(1L)).isTrue();
        long value = mapState.get(1L);
        assertThat(value).isEqualTo(5L);

        assertThat(mapState.contains(2L)).isTrue();
        value = mapState.get(2L);
        assertThat(value).isEqualTo(5L);

        assertThatThrownBy(() -> mapState.clear())
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void testIsEmpty() throws Exception {
        assertThat(mapState.isEmpty()).isFalse();
    }
}
