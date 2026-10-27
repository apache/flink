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
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.runtime.NullableSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TtlAwareSerializerTest {

    private static final String VALUE = "value";
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
                                .getOriginalTypeSerializerSnapshot())
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
        TtlAwareSerializer<?, ?> prior = stringSerializer(false);
        TtlAwareSerializer<?, ?> current = stringSerializer(false);

        assertThat(migrate(current, prior, VALUE)).isEqualTo(serialize(current, VALUE));
    }

    @Test
    void testMigrateValueNoTtlToTtlStampsCurrentTime() throws IOException {
        TtlAwareSerializer<?, ?> prior = stringSerializer(false);
        TtlAwareSerializer<?, ?> current = stringSerializer(true);

        assertThat(migrate(current, prior, VALUE))
                .isEqualTo(serialize(current, new TtlValue<>(VALUE, CURRENT_TIMESTAMP)));
    }

    @Test
    void testMigrateValueTtlToNoTtlUnwraps() throws IOException {
        TtlAwareSerializer<?, ?> prior = stringSerializer(true);
        TtlAwareSerializer<?, ?> current = stringSerializer(false);

        assertThat(migrate(current, prior, new TtlValue<>(VALUE, PRIOR_TIMESTAMP)))
                .isEqualTo(serialize(current, VALUE));
    }

    @Test
    void testMigrateValueTtlToTtlKeepsPriorTimestamp() throws IOException {
        TtlAwareSerializer<?, ?> prior = stringSerializer(true);
        TtlAwareSerializer<?, ?> current = stringSerializer(true);
        TtlValue<String> priorValue = new TtlValue<>(VALUE, PRIOR_TIMESTAMP);

        assertThat(migrate(current, prior, priorValue)).isEqualTo(serialize(current, priorValue));
    }

    @Test
    void testMigrateValueInvokesHookOnNewSnapshotWithOldSnapshotAsArgument() throws IOException {
        TtlAwareSerializer<?, ?> prior =
                TtlAwareSerializer.wrapTtlAwareSerializer(new TaggedStringSerializer("old"));
        TtlAwareSerializer<?, ?> current =
                TtlAwareSerializer.wrapTtlAwareSerializer(new TaggedStringSerializer("new"));

        byte[] migrated = migrate(current, prior, VALUE);

        assertThat(current.deserialize(new DataInputDeserializer(migrated)))
                .isEqualTo(VALUE + "|from=old|to=new");
    }

    @Test
    void testMigrateValueInvokesHookOnUnwrappedValueWithInnerSnapshots() throws IOException {
        TtlAwareSerializer<?, ?> prior = taggedTtlSerializer("old");
        TtlAwareSerializer<?, ?> current = taggedTtlSerializer("new");

        byte[] migrated = migrate(current, prior, new TtlValue<>(VALUE, PRIOR_TIMESTAMP));

        TtlValue<?> result = (TtlValue<?>) current.deserialize(new DataInputDeserializer(migrated));
        assertThat(result.getUserValue()).isEqualTo(VALUE + "|from=old|to=new");
        assertThat(result.getLastAccessTimestamp()).isEqualTo(PRIOR_TIMESTAMP);
    }

    @Test
    void testMigrateValueUsesPersistedPriorSnapshotNotOneRederivedFromPriorSerializer()
            throws IOException {
        TtlAwareSerializer<?, ?> prior =
                TtlAwareSerializer.wrapTtlAwareSerializer(new TaggedStringSerializer("rederived"));
        TtlAwareSerializer<?, ?> current =
                TtlAwareSerializer.wrapTtlAwareSerializer(new TaggedStringSerializer("new"));

        byte[] migrated = migrate(current, prior, new TaggedSnapshot("persisted"), VALUE);

        assertThat(current.deserialize(new DataInputDeserializer(migrated)))
                .isEqualTo(VALUE + "|from=persisted|to=new");
    }

    @Test
    void testMigrateValueDescendsPersistedTtlSnapshotToItsValueSnapshot() throws IOException {
        TtlAwareSerializer<?, ?> prior = taggedTtlSerializer("rederived");
        TtlAwareSerializer<?, ?> current = taggedTtlSerializer("new");
        // Tagged differently from the prior serializer, so both falling back to a re-derived
        // snapshot and skipping the descent into the TtlValue envelope change the result.
        TypeSerializerSnapshot<?> persisted =
                taggedTtlSerializer("persisted")
                        .getOriginalTypeSerializer()
                        .snapshotConfiguration();

        byte[] migrated =
                migrate(current, prior, persisted, new TtlValue<>(VALUE, PRIOR_TIMESTAMP));

        TtlValue<?> result = (TtlValue<?>) current.deserialize(new DataInputDeserializer(migrated));
        assertThat(result.getUserValue()).isEqualTo(VALUE + "|from=persisted|to=new");
    }

    /**
     * A list or map state persists its element or value snapshot wrapped in a {@link
     * TtlAwareSerializerSnapshot}, which the descent has to see through to reach the user value
     * snapshot. Value state persists the unwrapped shape covered by the test above.
     */
    @Test
    void testMigrateValueDescendsPersistedTtlAwareElementSnapshot() throws IOException {
        ListSerializerSnapshot<?> persistedListSnapshot =
                (ListSerializerSnapshot<?>)
                        TtlAwareSerializer.wrapTtlAwareSerializer(
                                        new ListSerializer<>(rawTaggedTtlSerializer("persisted")))
                                .snapshotConfiguration();
        TypeSerializerSnapshot<?> persistedElementSnapshot =
                persistedListSnapshot.getElementSerializerSnapshot();
        assertThat(persistedElementSnapshot).isInstanceOf(TtlAwareSerializerSnapshot.class);

        TtlAwareSerializer<?, ?> prior = taggedTtlSerializer("rederived");
        TtlAwareSerializer<?, ?> current = taggedTtlSerializer("new");

        byte[] migrated =
                migrate(
                        current,
                        prior,
                        persistedElementSnapshot,
                        new TtlValue<>(VALUE, PRIOR_TIMESTAMP));

        TtlValue<?> result = (TtlValue<?>) current.deserialize(new DataInputDeserializer(migrated));
        assertThat(result.getUserValue()).isEqualTo(VALUE + "|from=persisted|to=new");
    }

    @Test
    void testMigrateValueRejectsNonTtlSnapshotForTtlPriorSerializer() {
        TtlAwareSerializer<?, ?> prior = taggedTtlSerializer("old");
        TtlAwareSerializer<?, ?> current = taggedTtlSerializer("new");

        assertThatThrownBy(
                        () ->
                                migrate(
                                        current,
                                        prior,
                                        new TaggedSnapshot("mismatched"),
                                        new TtlValue<>(VALUE, PRIOR_TIMESTAMP)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("should be a TtlSerializerSnapshot");
    }

    @Test
    void testMigrateValueRejectsNonTtlSnapshotBehindTtlAwareLayer() {
        TtlAwareSerializer<?, ?> prior = taggedTtlSerializer("old");
        TtlAwareSerializer<?, ?> current = taggedTtlSerializer("new");
        // The TtlAware layer is seen through, so a mismatch inside it is still caught.
        TypeSerializerSnapshot<?> wrappedMismatch =
                new TtlAwareSerializerSnapshot<>(new TaggedSnapshot("mismatched"));

        assertThatThrownBy(
                        () ->
                                migrate(
                                        current,
                                        prior,
                                        wrappedMismatch,
                                        new TtlValue<>(VALUE, PRIOR_TIMESTAMP)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("should be a TtlSerializerSnapshot");
    }

    @Test
    void testMigrateValueRejectsTtlSnapshotForNonTtlPriorSerializer() {
        TtlAwareSerializer<?, ?> prior =
                TtlAwareSerializer.wrapTtlAwareSerializer(new TaggedStringSerializer("old"));
        TtlAwareSerializer<?, ?> current =
                TtlAwareSerializer.wrapTtlAwareSerializer(new TaggedStringSerializer("new"));
        TypeSerializerSnapshot<?> ttlSnapshot =
                rawTaggedTtlSerializer("mismatched").snapshotConfiguration();

        assertThatThrownBy(() -> migrate(current, prior, ttlSnapshot, VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("does not wrap values in TtlValue");
    }

    @Test
    void testMigrateValueFallsBackToPriorSerializerSnapshotWhenNonePersisted() throws IOException {
        TtlAwareSerializer<?, ?> prior =
                TtlAwareSerializer.wrapTtlAwareSerializer(new TaggedStringSerializer("old"));
        TtlAwareSerializer<?, ?> current =
                TtlAwareSerializer.wrapTtlAwareSerializer(new TaggedStringSerializer("new"));

        byte[] migrated = migrate(current, prior, null, VALUE);

        assertThat(current.deserialize(new DataInputDeserializer(migrated)))
                .isEqualTo(VALUE + "|from=old|to=new");
    }

    @Test
    void testMigrateValueTtlToTtlWithNullUserValue() throws IOException {
        TtlAwareSerializer<?, ?> prior = nullTolerantTtlSerializer();
        TtlAwareSerializer<?, ?> current = nullTolerantTtlSerializer();

        byte[] migrated = migrate(current, prior, new TtlValue<>(null, PRIOR_TIMESTAMP));

        TtlValue<?> result = (TtlValue<?>) current.deserialize(new DataInputDeserializer(migrated));
        assertThat(result.getUserValue()).isNull();
        assertThat(result.getLastAccessTimestamp()).isEqualTo(PRIOR_TIMESTAMP);
    }

    /** Migrates with the snapshot the state backends would have persisted for {@code prior}. */
    private static byte[] migrate(
            TtlAwareSerializer<?, ?> current, TtlAwareSerializer<?, ?> prior, Object priorValue)
            throws IOException {
        return migrate(
                current,
                prior,
                prior.getOriginalTypeSerializer().snapshotConfiguration(),
                priorValue);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static byte[] migrate(
            TtlAwareSerializer<?, ?> current,
            TtlAwareSerializer<?, ?> prior,
            TypeSerializerSnapshot<?> priorSnapshot,
            Object priorValue)
            throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(64);
        ((TtlAwareSerializer) current)
                .migrateValueFromPriorSerializer(
                        (TtlAwareSerializer) prior,
                        priorSnapshot,
                        () -> priorValue,
                        output,
                        FIXED_TIME_PROVIDER);
        return output.getCopyOfBuffer();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static byte[] serialize(TtlAwareSerializer<?, ?> serializer, Object value)
            throws IOException {
        DataOutputSerializer output = new DataOutputSerializer(64);
        ((TtlAwareSerializer) serializer).serialize(value, output);
        return output.getCopyOfBuffer();
    }

    private static TtlAwareSerializer<?, ?> stringSerializer(boolean ttlEnabled) {
        return ttlEnabled
                ? TtlAwareSerializer.wrapTtlAwareSerializer(
                        new TtlStateFactory.TtlSerializer<>(
                                LongSerializer.INSTANCE, StringSerializer.INSTANCE))
                : TtlAwareSerializer.wrapTtlAwareSerializer(StringSerializer.INSTANCE);
    }

    private static TtlAwareSerializer<?, ?> taggedTtlSerializer(String tag) {
        return TtlAwareSerializer.wrapTtlAwareSerializer(rawTaggedTtlSerializer(tag));
    }

    private static TtlStateFactory.TtlSerializer<String> rawTaggedTtlSerializer(String tag) {
        return new TtlStateFactory.TtlSerializer<>(
                LongSerializer.INSTANCE, new TaggedStringSerializer(tag));
    }

    private static TtlAwareSerializer<?, ?> nullTolerantTtlSerializer() {
        return TtlAwareSerializer.wrapTtlAwareSerializer(
                new TtlStateFactory.TtlSerializer<>(
                        LongSerializer.INSTANCE,
                        NullableSerializer.wrapIfNullIsNotSupported(
                                StringSerializer.INSTANCE, false)));
    }

    /** A string serializer whose snapshot records the schema it belongs to. */
    private static final class TaggedStringSerializer extends TypeSerializer<String> {

        private final String tag;

        private TaggedStringSerializer(String tag) {
            this.tag = tag;
        }

        @Override
        public boolean isImmutableType() {
            return true;
        }

        @Override
        public TypeSerializer<String> duplicate() {
            return this;
        }

        @Override
        public String createInstance() {
            return "";
        }

        @Override
        public String copy(String from) {
            return from;
        }

        @Override
        public String copy(String from, String reuse) {
            return from;
        }

        @Override
        public int getLength() {
            return -1;
        }

        @Override
        public void serialize(String record, DataOutputView target) throws IOException {
            StringSerializer.INSTANCE.serialize(record, target);
        }

        @Override
        public String deserialize(DataInputView source) throws IOException {
            return StringSerializer.INSTANCE.deserialize(source);
        }

        @Override
        public String deserialize(String reuse, DataInputView source) throws IOException {
            return deserialize(source);
        }

        @Override
        public void copy(DataInputView source, DataOutputView target) throws IOException {
            StringSerializer.INSTANCE.copy(source, target);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof TaggedStringSerializer
                    && tag.equals(((TaggedStringSerializer) obj).tag);
        }

        @Override
        public int hashCode() {
            return tag.hashCode();
        }

        @Override
        public TypeSerializerSnapshot<String> snapshotConfiguration() {
            return new TaggedSnapshot(tag);
        }
    }

    /**
     * Appends both the argument snapshot's tag and its own tag to the migrated value, so a
     * migration that swaps receiver and argument produces a different result.
     */
    public static final class TaggedSnapshot implements TypeSerializerSnapshot<String> {

        private String tag;

        public TaggedSnapshot() {}

        private TaggedSnapshot(String tag) {
            this.tag = tag;
        }

        @Override
        public int getCurrentVersion() {
            return 1;
        }

        @Override
        public void writeSnapshot(DataOutputView out) throws IOException {
            out.writeUTF(tag);
        }

        @Override
        public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
                throws IOException {
            tag = in.readUTF();
        }

        @Override
        public TypeSerializer<String> restoreSerializer() {
            return new TaggedStringSerializer(tag);
        }

        @Override
        public TypeSerializerSchemaCompatibility<String> resolveSchemaCompatibility(
                TypeSerializerSnapshot<String> oldSerializerSnapshot) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }

        @Override
        public String migrate(TypeSerializerSnapshot<String> oldSerializerSnapshot, String value) {
            return value + "|from=" + ((TaggedSnapshot) oldSerializerSnapshot).tag + "|to=" + tag;
        }
    }
}
