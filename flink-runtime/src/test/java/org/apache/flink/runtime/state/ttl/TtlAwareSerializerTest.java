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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

class TtlAwareSerializerTest {

    private static final long PRIOR_TIMESTAMP = 1_000L;
    private static final long CURRENT_TIMESTAMP = 9_999L;
    private static final TtlTimeProvider FIXED_TIME_PROVIDER = () -> CURRENT_TIMESTAMP;

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

        TtlAwareSerializer<?, ?> intTtlAwareSerializer =
                TtlAwareSerializer.wrapTtlAwareSerializer(intSerializer);
        TtlAwareSerializer<?, ?> listTtlAwareSerializer =
                TtlAwareSerializer.wrapTtlAwareSerializer(listSerializer);
        TtlAwareSerializer<?, ?> mapTtlAwareSerializer =
                TtlAwareSerializer.wrapTtlAwareSerializer(mapSerializer);

        assertThat(intTtlAwareSerializer.isTtlEnabled()).isFalse();
        assertThat(listTtlAwareSerializer)
                .isInstanceOf(TtlAwareSerializer.TtlAwareListSerializer.class);
        assertThat((listTtlAwareSerializer).isTtlEnabled()).isFalse();
        assertThat(mapTtlAwareSerializer)
                .isInstanceOf(TtlAwareSerializer.TtlAwareMapSerializer.class);
        assertThat(mapTtlAwareSerializer.isTtlEnabled()).isFalse();
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

        TtlAwareSerializer<?, ?> intTtlAwareSerializer =
                TtlAwareSerializer.wrapTtlAwareSerializer(intTtlSerializer);
        TtlAwareSerializer<?, ?> listTtlAwareSerializer =
                TtlAwareSerializer.wrapTtlAwareSerializer(listTtlSerializer);
        TtlAwareSerializer<?, ?> mapTtlAwareSerializer =
                TtlAwareSerializer.wrapTtlAwareSerializer(mapTtlSerializer);

        assertThat((intTtlAwareSerializer).isTtlEnabled()).isTrue();
        assertThat(listTtlAwareSerializer)
                .isInstanceOf(TtlAwareSerializer.TtlAwareListSerializer.class);
        assertThat((listTtlAwareSerializer).isTtlEnabled()).isTrue();
        assertThat(mapTtlAwareSerializer)
                .isInstanceOf(TtlAwareSerializer.TtlAwareMapSerializer.class);
        assertThat(mapTtlAwareSerializer.isTtlEnabled()).isTrue();
    }

    @Test
    @SuppressWarnings("rawtypes")
    void testSnapshotConfiguration() {
        TtlAwareSerializer<?, ?> intTtlAwareSerializer =
                TtlAwareSerializer.wrapTtlAwareSerializer(IntSerializer.INSTANCE);
        TtlAwareSerializer.TtlAwareListSerializer<?> listTtlAwareSerializer =
                (TtlAwareSerializer.TtlAwareListSerializer<?>)
                        TtlAwareSerializer.wrapTtlAwareSerializer(
                                new ListSerializer<>(IntSerializer.INSTANCE));
        TtlAwareSerializer.TtlAwareMapSerializer<?, ?> mapTtlAwareSerializer =
                (TtlAwareSerializer.TtlAwareMapSerializer<?, ?>)
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

    @Test
    void testMigrateValueNoTtlToNoTtl() throws IOException {
        Object result = migrate(false, false, "value");
        assertThat(result).isEqualTo("value");
    }

    @Test
    void testMigrateValueNoTtlToTtl() throws IOException {
        Object result = migrate(true, false, "value");
        assertThat(result).isInstanceOf(TtlValue.class);
        TtlValue<?> ttlValue = (TtlValue<?>) result;
        assertThat(ttlValue.getUserValue()).isEqualTo("value");
        assertThat(ttlValue.getLastAccessTimestamp()).isEqualTo(CURRENT_TIMESTAMP);
    }

    @Test
    void testMigrateValueTtlToNoTtl() throws IOException {
        Object result = migrate(false, true, new TtlValue<>("value", PRIOR_TIMESTAMP));
        assertThat(result).isEqualTo("value");
    }

    @Test
    void testMigrateValueTtlToTtlPreservesTimestamp() throws IOException {
        Object result = migrate(true, true, new TtlValue<>("value", PRIOR_TIMESTAMP));
        assertThat(result).isInstanceOf(TtlValue.class);
        TtlValue<?> ttlValue = (TtlValue<?>) result;
        assertThat(ttlValue.getUserValue()).isEqualTo("value");
        // The prior timestamp must be preserved, not reset to the current time.
        assertThat(ttlValue.getLastAccessTimestamp()).isEqualTo(PRIOR_TIMESTAMP);
    }

    /**
     * Runs {@code migrateValueFromPriorSerializer} for a non-RowData inner value type across a
     * given (current TTL, prior TTL) combination and returns the value read back with the current
     * serializer. {@code priorValue} is the bare {@code String} when {@code priorTtlEnabled} is
     * false, or a {@code TtlValue<String>} when it is true.
     */
    private static Object migrate(
            boolean currentTtlEnabled, boolean priorTtlEnabled, Object priorValue)
            throws IOException {
        TtlAwareSerializer<?, ?> prior = stringSerializer(priorTtlEnabled);
        TtlAwareSerializer<?, ?> current = stringSerializer(currentTtlEnabled);

        DataOutputSerializer output = new DataOutputSerializer(64);
        @SuppressWarnings({"unchecked", "rawtypes"})
        TtlAwareSerializer<Object, ?> currentRaw = (TtlAwareSerializer) current;
        @SuppressWarnings({"unchecked", "rawtypes"})
        TtlAwareSerializer<Object, ?> priorRaw = (TtlAwareSerializer) prior;
        currentRaw.migrateValueFromPriorSerializer(
                priorRaw, () -> priorValue, output, FIXED_TIME_PROVIDER);

        return current.deserialize(new DataInputDeserializer(output.getCopyOfBuffer()));
    }

    private static TtlAwareSerializer<?, ?> stringSerializer(boolean ttlEnabled) {
        if (ttlEnabled) {
            return TtlAwareSerializer.wrapTtlAwareSerializer(
                    new TtlStateFactory.TtlSerializer<>(
                            LongSerializer.INSTANCE, StringSerializer.INSTANCE));
        }
        return TtlAwareSerializer.wrapTtlAwareSerializer(StringSerializer.INSTANCE);
    }
}
