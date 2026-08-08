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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the lenient POJO class-loading in {@link PojoSerializerSnapshotData}: a {@link
 * PojoSerializerSnapshot} must be readable even when the POJO class is not on the classpath, with
 * the class name, the field names, and the field serializer snapshots all remaining accessible.
 */
class PojoSerializerSnapshotLenientReadTest {

    private static final Map<String, Class<?>> EXPECTED_FIELD_SNAPSHOTS =
            Map.of(
                    "name", StringSerializer.StringSerializerSnapshot.class,
                    "age", IntSerializer.IntSerializerSnapshot.class,
                    "score", LongSerializer.LongSerializerSnapshot.class);

    /** POJO present while the snapshot is written, hidden from the classloader while it is read. */
    public static class SomePojo {
        public String name;
        public int age;
        public long score;
    }

    @Test
    void testReadWithClassPresent() throws IOException {
        PojoSerializerSnapshot<SomePojo> read =
                roundtripSnapshot(writeSnapshot(), getClass().getClassLoader());

        assertThat(read.isPojoClassAvailable()).isTrue();
        assertThat(read.getPojoClassName()).isEqualTo(SomePojo.class.getName());
        assertFieldSnapshots(read);
    }

    @Test
    void testReadWithClassAbsent() throws IOException {
        // Hides both the POJO class itself and the declaring class of each of its fields.
        ClassLoader withoutPojo =
                new ClassLoader(getClass().getClassLoader()) {
                    @Override
                    protected Class<?> loadClass(String name, boolean resolve)
                            throws ClassNotFoundException {
                        if (name.contains(SomePojo.class.getSimpleName())) {
                            throw new ClassNotFoundException(name);
                        }
                        return super.loadClass(name, resolve);
                    }
                };

        PojoSerializerSnapshot<SomePojo> read = roundtripSnapshot(writeSnapshot(), withoutPojo);

        assertThat(read.isPojoClassAvailable()).isFalse();
        assertThat(read.getPojoClassName()).isEqualTo(SomePojo.class.getName());
        // Field names stay available because the key name is written before the framed value.
        assertFieldSnapshots(read);
    }

    private static void assertFieldSnapshots(PojoSerializerSnapshot<SomePojo> snapshot) {
        List<SimpleEntry<String, TypeSerializerSnapshot<?>>> entries =
                snapshot.getFieldSnapshotEntries();

        assertThat(entries).hasSize(EXPECTED_FIELD_SNAPSHOTS.size());
        for (SimpleEntry<String, TypeSerializerSnapshot<?>> entry : entries) {
            Class<?> expectedType = EXPECTED_FIELD_SNAPSHOTS.get(entry.getKey());
            assertThat(expectedType).as("unexpected field '%s'", entry.getKey()).isNotNull();
            assertThat(entry.getValue())
                    .as("snapshot of field '%s'", entry.getKey())
                    .isExactlyInstanceOf(expectedType);
        }
    }

    @SuppressWarnings("unchecked")
    private static PojoSerializerSnapshot<SomePojo> writeSnapshot() {
        return (PojoSerializerSnapshot<SomePojo>)
                TypeExtractor.createTypeInfo(SomePojo.class)
                        .createSerializer(new SerializerConfigImpl())
                        .snapshotConfiguration();
    }

    @SuppressWarnings("unchecked")
    private static PojoSerializerSnapshot<SomePojo> roundtripSnapshot(
            PojoSerializerSnapshot<SomePojo> snapshot, ClassLoader classLoader) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, snapshot);

        DataInputDeserializer in = new DataInputDeserializer(out.getSharedBuffer());
        return (PojoSerializerSnapshot<SomePojo>)
                TypeSerializerSnapshot.<SomePojo>readVersionedSnapshot(in, classLoader);
    }
}
