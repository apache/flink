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

package org.apache.flink.state.api.input.deserializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.EnumSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Arrays;

/**
 * A {@link TypeSerializer} that reads the enum ordinal written by {@link EnumSerializer} and
 * produces the enum constant's {@code name()} as a plain {@link String}, without the user enum
 * class being on the classpath.
 *
 * <p>{@link EnumSerializer} writes a maintained ordinal (see {@link
 * EnumSerializer.EnumSerializerSnapshot#getEnumNames()}) rather than {@link Enum#ordinal()}, so the
 * name lookup here uses the exact same array the original serializer would have used to resolve
 * that ordinal back to a constant.
 */
@Internal
public final class EnumNameDeserializer extends TypeSerializer<String> {

    private static final long serialVersionUID = 1L;

    private final String[] enumNames;

    public static EnumNameDeserializer create(EnumSerializer.EnumSerializerSnapshot<?> snapshot) {
        return new EnumNameDeserializer(snapshot.getEnumNames());
    }

    EnumNameDeserializer(String[] enumNames) {
        this.enumNames = enumNames;
    }

    @Override
    public String deserialize(DataInputView source) throws IOException {
        int ordinal = source.readInt();
        if (ordinal < 0 || ordinal >= enumNames.length) {
            throw new IOException(
                    "Unknown enum ordinal "
                            + ordinal
                            + " (have "
                            + enumNames.length
                            + " known constants). The savepoint may have been written with a"
                            + " different enum definition.");
        }
        return enumNames[ordinal];
    }

    @Override
    public String deserialize(String reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    // -------------------------------------------------------------------------
    // TypeSerializer boilerplate — copy/snapshot operations not needed for
    // schema-extraction use cases but required by the interface.
    // -------------------------------------------------------------------------

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
        return enumNames.length > 0 ? enumNames[0] : null;
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
        return 4;
    }

    @Override
    public void serialize(String record, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException(
                "EnumNameDeserializer is read-only; serialization is not supported.");
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        throw new UnsupportedOperationException(
                "EnumNameDeserializer is read-only; copy is not supported.");
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof EnumNameDeserializer)) {
            return false;
        }
        return Arrays.equals(enumNames, ((EnumNameDeserializer) obj).enumNames);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(enumNames);
    }

    @Override
    public TypeSerializerSnapshot<String> snapshotConfiguration() {
        throw new UnsupportedOperationException(
                "EnumNameDeserializer is only ever used directly within a single read; it is never"
                        + " re-snapshotted.");
    }
}
