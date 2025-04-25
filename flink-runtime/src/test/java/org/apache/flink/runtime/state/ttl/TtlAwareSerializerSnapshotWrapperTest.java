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

import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for {@link TtlAwareSerializerSnapshotWrapper}. */
public class TtlAwareSerializerSnapshotWrapperTest {
    @Test
    public void testValueStateTtlAwareSerializerSnapshot() {
        TypeSerializerSnapshot<Integer> intSerializerSnapshot =
                IntSerializer.INSTANCE.snapshotConfiguration();
        TypeSerializerSnapshot<Integer> serializerSnapshot =
                new TtlAwareSerializerSnapshotWrapper<>(intSerializerSnapshot)
                        .getTtlAwareSerializerSnapshot();
        assertThat(serializerSnapshot).isInstanceOf(TtlAwareSerializerSnapshot.class);
        assertThat(
                        ((TtlAwareSerializer<Integer, IntSerializer>)
                                        serializerSnapshot.restoreSerializer())
                                .getOriginalTypeSerializer())
                .isInstanceOf(IntSerializer.class);
    }

    @Test
    public void testRestoreValueSerializer() {
        TypeSerializerSnapshot<Integer> intSerializerSnapshot =
                IntSerializer.INSTANCE.snapshotConfiguration();
        TypeSerializerSnapshot<Integer> serializerSnapshot =
                (new TtlAwareSerializerSnapshotWrapper<>(intSerializerSnapshot))
                        .getTtlAwareSerializerSnapshot();
        assertThat(serializerSnapshot.restoreSerializer()).isInstanceOf(TtlAwareSerializer.class);
        assertThat(
                        ((TtlAwareSerializer<Integer, IntSerializer>)
                                        serializerSnapshot.restoreSerializer())
                                .getOriginalTypeSerializer())
                .isInstanceOf(IntSerializer.class);
    }

    @Test
    public void testListStateTtlAwareSerializerSnapshot() {
        ListSerializer<Integer> listSerializer = new ListSerializer<>(IntSerializer.INSTANCE);
        TypeSerializerSnapshot<List<Integer>> listTypeSerializerSnapshot =
                listSerializer.snapshotConfiguration();
        TypeSerializerSnapshot<List<Integer>> serializerSnapshot =
                (new TtlAwareSerializerSnapshotWrapper<>(listTypeSerializerSnapshot))
                        .getTtlAwareSerializerSnapshot();

        assertThat(serializerSnapshot).isInstanceOf(ListSerializerSnapshot.class);
        assertThat(
                        ((ListSerializerSnapshot<Integer>) serializerSnapshot)
                                .getElementSerializerSnapshot())
                .isInstanceOf(TtlAwareSerializerSnapshot.class);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testRestoreListSerializer() {
        ListSerializer<Integer> listSerializer = new ListSerializer<>(IntSerializer.INSTANCE);
        TypeSerializerSnapshot<List<Integer>> listTypeSerializerSnapshot =
                listSerializer.snapshotConfiguration();
        TypeSerializerSnapshot<List<Integer>> serializerSnapshot =
                (new TtlAwareSerializerSnapshotWrapper<>(listTypeSerializerSnapshot))
                        .getTtlAwareSerializerSnapshot();

        assertThat(serializerSnapshot.restoreSerializer()).isInstanceOf(ListSerializer.class);
        assertThat(((ListSerializer) serializerSnapshot.restoreSerializer()).getElementSerializer())
                .isInstanceOf(TtlAwareSerializer.class);
        assertThat(
                        ((TtlAwareSerializer)
                                        ((ListSerializer) serializerSnapshot.restoreSerializer())
                                                .getElementSerializer())
                                .getOriginalTypeSerializer())
                .isInstanceOf(IntSerializer.class);
    }

    @Test
    public void testMapStateTtlAwareSerializerSnapshot() {
        MapSerializer<String, String> mapSerializer =
                new MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE);
        TypeSerializerSnapshot<Map<String, String>> mapSerializerSnapshot =
                mapSerializer.snapshotConfiguration();
        TypeSerializerSnapshot<Map<String, String>> serializerSnapshot =
                (new TtlAwareSerializerSnapshotWrapper<>(mapSerializerSnapshot))
                        .getTtlAwareSerializerSnapshot();

        assertThat(serializerSnapshot).isInstanceOf(MapSerializerSnapshot.class);
        assertThat(
                        ((MapSerializerSnapshot<String, String>) serializerSnapshot)
                                .getValueSerializerSnapshot())
                .isInstanceOf(TtlAwareSerializerSnapshot.class);
    }

    @Test
    @SuppressWarnings("rawtypes")
    public void testRestoreMapSerializer() {
        MapSerializer<String, String> mapSerializer =
                new MapSerializer<>(StringSerializer.INSTANCE, StringSerializer.INSTANCE);
        TypeSerializerSnapshot<Map<String, String>> mapSerializerSnapshot =
                mapSerializer.snapshotConfiguration();
        TypeSerializerSnapshot<Map<String, String>> serializerSnapshot =
                (new TtlAwareSerializerSnapshotWrapper<>(mapSerializerSnapshot))
                        .getTtlAwareSerializerSnapshot();

        assertThat(serializerSnapshot.restoreSerializer()).isInstanceOf(MapSerializer.class);
        assertThat(((MapSerializer) serializerSnapshot.restoreSerializer()).getValueSerializer())
                .isInstanceOf(TtlAwareSerializer.class);
        assertThat(
                        ((TtlAwareSerializer)
                                        ((MapSerializer) serializerSnapshot.restoreSerializer())
                                                .getValueSerializer())
                                .getOriginalTypeSerializer())
                .isInstanceOf(StringSerializer.class);
    }
}
