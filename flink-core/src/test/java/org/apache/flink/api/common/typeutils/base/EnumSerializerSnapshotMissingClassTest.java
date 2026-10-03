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

package org.apache.flink.api.common.typeutils.base;

import org.apache.flink.api.common.typeutils.CustomRestoreSerializerFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.EnumSerializer.EnumSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the lenient enum class-loading in {@link EnumSerializerSnapshot}: with a {@link
 * CustomRestoreSerializerFactory} registered, an {@link EnumSerializerSnapshot} must be readable
 * even when the enum class is not on the classpath, with the constant names (in wire-ordinal order)
 * remaining accessible.
 */
class EnumSerializerSnapshotMissingClassTest {

    enum SomeEnum {
        FOO,
        BAR,
        BAZ
    }

    @Test
    void testReadWithClassAbsent() throws IOException {
        EnumSerializerSnapshot<SomeEnum> read;
        CustomRestoreSerializerFactory.set(
                snapshot -> {
                    throw new UnsupportedOperationException("not exercised in this test");
                });
        try {
            read = roundtripSnapshot(writeSnapshot(), withoutEnumClassLoader());
        } finally {
            CustomRestoreSerializerFactory.remove();
        }

        assertThat(read.getEnumNames()).containsExactly("FOO", "BAR", "BAZ");
    }

    /**
     * Regular job restores (i.e. without a {@link CustomRestoreSerializerFactory} registered, as is
     * always the case outside of the State Processing API) must still fail fast when the enum class
     * is genuinely missing.
     */
    @Test
    void testReadFailsFastWithoutFallbackFactory() {
        assertThatThrownBy(() -> roundtripSnapshot(writeSnapshot(), withoutEnumClassLoader()))
                .isInstanceOf(NoClassDefFoundError.class)
                .hasCauseInstanceOf(ClassNotFoundException.class);
    }

    /** Hides the enum class from the classloader used to read the snapshot back. */
    private ClassLoader withoutEnumClassLoader() {
        return new ClassLoader(getClass().getClassLoader()) {
            @Override
            protected Class<?> loadClass(String name, boolean resolve)
                    throws ClassNotFoundException {
                if (name.contains(SomeEnum.class.getSimpleName())) {
                    throw new ClassNotFoundException(name);
                }
                return super.loadClass(name, resolve);
            }
        };
    }

    private static EnumSerializerSnapshot<SomeEnum> writeSnapshot() {
        return new EnumSerializer<>(SomeEnum.class).snapshotConfiguration();
    }

    @SuppressWarnings("unchecked")
    private static EnumSerializerSnapshot<SomeEnum> roundtripSnapshot(
            EnumSerializerSnapshot<SomeEnum> snapshot, ClassLoader classLoader) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        TypeSerializerSnapshot.writeVersionedSnapshot(out, snapshot);

        DataInputDeserializer in = new DataInputDeserializer(out.getSharedBuffer());
        return (EnumSerializerSnapshot<SomeEnum>)
                TypeSerializerSnapshot.<SomeEnum>readVersionedSnapshot(in, classLoader);
    }
}
