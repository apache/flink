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

package org.apache.flink.runtime.state.ttl;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TtlAwareSerializerTest {

    @Test
    void testSerializerTtlEnabled() {
        IntSerializer intSerializer = IntSerializer.INSTANCE;
        ListSerializer<Integer> listSerializer = new ListSerializer<>(intSerializer);
        MapSerializer<Integer, Integer> mapSerializer =
                new MapSerializer<>(intSerializer, intSerializer);

        assertThat(TtlAwareSerializer.isSerializerTtlEnabled(intSerializer)).isFalse();
        assertThat(TtlAwareSerializer.isSerializerTtlEnabled(listSerializer)).isFalse();
        assertThat(TtlAwareSerializer.isSerializerTtlEnabled(mapSerializer)).isFalse();

        TtlStateFactory.TtlSerializer<Integer> intTtlSerializer =
                new TtlStateFactory.TtlSerializer<>(LongSerializer.INSTANCE, intSerializer);
        ListSerializer<TtlValue<Integer>> listTtlSerializer =
                new ListSerializer<>(intTtlSerializer);
        MapSerializer<Integer, TtlValue<Integer>> mapTtlSerializer =
                new MapSerializer<>(intSerializer, intTtlSerializer);

        assertThat(TtlAwareSerializer.isSerializerTtlEnabled(intTtlSerializer)).isTrue();
        assertThat(TtlAwareSerializer.isSerializerTtlEnabled(listTtlSerializer)).isTrue();
        assertThat(TtlAwareSerializer.isSerializerTtlEnabled(mapTtlSerializer)).isTrue();

        assertThat(TtlAwareSerializer.needTtlStateMigration(intSerializer, intTtlSerializer))
                .isTrue();
        assertThat(TtlAwareSerializer.needTtlStateMigration(listSerializer, listTtlSerializer))
                .isTrue();
        assertThat(TtlAwareSerializer.needTtlStateMigration(mapSerializer, mapTtlSerializer))
                .isTrue();
    }

    @Test
    void testWrapTypeSerializer() {
        IntSerializer intSerializer = IntSerializer.INSTANCE;
        ListSerializer<Integer> listSerializer = new ListSerializer<>(intSerializer);
        MapSerializer<Integer, Integer> mapSerializer =
                new MapSerializer<>(intSerializer, intSerializer);

        TypeSerializer<?> intTtlAwareSerializer =
                TtlAwareSerializer.wrapTtlAwareSerializer(intSerializer);
        ListSerializer<?> listTtlAwareSerializer =
                (ListSerializer<?>) TtlAwareSerializer.wrapTtlAwareSerializer(listSerializer);
        MapSerializer<?, ?> mapTtlAwareSerializer =
                (MapSerializer<?, ?>) TtlAwareSerializer.wrapTtlAwareSerializer(mapSerializer);

        assertThat(intTtlAwareSerializer).isInstanceOf(TtlAwareSerializer.class);
        assertThat(((TtlAwareSerializer<?>) intTtlAwareSerializer).isTtlEnabled()).isFalse();
        assertThat(listTtlAwareSerializer.getElementSerializer())
                .isInstanceOf(TtlAwareSerializer.class);
        assertThat(
                        ((TtlAwareSerializer<?>) listTtlAwareSerializer.getElementSerializer())
                                .isTtlEnabled())
                .isFalse();
        assertThat(mapTtlAwareSerializer.getValueSerializer())
                .isInstanceOf(TtlAwareSerializer.class);
        assertThat(
                        ((TtlAwareSerializer<?>) mapTtlAwareSerializer.getValueSerializer())
                                .isTtlEnabled())
                .isFalse();
    }

    @Test
    void testWrapTtlSerializer() {
        TtlStateFactory.TtlSerializer<Integer> intTtlSerializer =
                new TtlStateFactory.TtlSerializer<>(
                        LongSerializer.INSTANCE, IntSerializer.INSTANCE);
        ListSerializer<TtlValue<Integer>> listTtlSerializer =
                new ListSerializer<>(intTtlSerializer);
        MapSerializer<Integer, TtlValue<Integer>> mapTtlSerializer =
                new MapSerializer<>(IntSerializer.INSTANCE, intTtlSerializer);

        TypeSerializer<?> intTtlAwareSerializer =
                TtlAwareSerializer.wrapTtlAwareSerializer(intTtlSerializer);
        ListSerializer<?> listTtlAwareSerializer =
                (ListSerializer<?>) TtlAwareSerializer.wrapTtlAwareSerializer(listTtlSerializer);
        MapSerializer<?, ?> mapTtlAwareSerializer =
                (MapSerializer<?, ?>) TtlAwareSerializer.wrapTtlAwareSerializer(mapTtlSerializer);

        assertThat(intTtlAwareSerializer).isInstanceOf(TtlAwareSerializer.class);
        assertThat(((TtlAwareSerializer<?>) intTtlAwareSerializer).isTtlEnabled()).isTrue();
        assertThat(listTtlAwareSerializer.getElementSerializer())
                .isInstanceOf(TtlAwareSerializer.class);
        assertThat(
                        ((TtlAwareSerializer<?>) listTtlAwareSerializer.getElementSerializer())
                                .isTtlEnabled())
                .isTrue();
        assertThat(mapTtlAwareSerializer.getValueSerializer())
                .isInstanceOf(TtlAwareSerializer.class);
        assertThat(
                        ((TtlAwareSerializer<?>) mapTtlAwareSerializer.getValueSerializer())
                                .isTtlEnabled())
                .isTrue();
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testSnapshotConfiguration() {
        TypeSerializer<?> intTtlAwareSerializer =
                TtlAwareSerializer.wrapTtlAwareSerializer(IntSerializer.INSTANCE);
        ListSerializer<?> listTtlAwareSerializer =
                (ListSerializer<?>)
                        TtlAwareSerializer.wrapTtlAwareSerializer(
                                new ListSerializer<>(IntSerializer.INSTANCE));
        MapSerializer<?, ?> mapTtlAwareSerializer =
                (MapSerializer<?, ?>)
                        TtlAwareSerializer.wrapTtlAwareSerializer(
                                new MapSerializer<>(
                                        IntSerializer.INSTANCE, IntSerializer.INSTANCE));

        assertThat(intTtlAwareSerializer.snapshotConfiguration())
                .isInstanceOf(TtlAwareSerializerSnapshot.class);
        assertThat(
                        ((TtlAwareSerializerSnapshot<?>)
                                        intTtlAwareSerializer.snapshotConfiguration())
                                .getOrinalTypeSerializerSnapshot())
                .isInstanceOf(IntSerializer.IntSerializerSnapshot.class);

        assertThat(listTtlAwareSerializer.snapshotConfiguration())
                .isInstanceOf(ListSerializerSnapshot.class);
        assertThat(
                        (((ListSerializerSnapshot) listTtlAwareSerializer.snapshotConfiguration())
                                .getElementSerializerSnapshot()))
                .isInstanceOf(TtlAwareSerializerSnapshot.class);

        assertThat(mapTtlAwareSerializer.snapshotConfiguration())
                .isInstanceOf(MapSerializerSnapshot.class);
        assertThat(
                        (((MapSerializerSnapshot) mapTtlAwareSerializer.snapshotConfiguration())
                                .getValueSerializerSnapshot()))
                .isInstanceOf(TtlAwareSerializerSnapshot.class);
    }
}
