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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base {@link TypeSerializerSnapshot} for serializers for generic types.
 *
 * @param <T> The type to be instantiated.
 */
@Internal
public abstract class GenericTypeSerializerSnapshot<T, S extends TypeSerializer>
        implements TypeSerializerSnapshot<T> {

    private static final int VERSION = 2;
    private Class<T> typeClass;

    protected GenericTypeSerializerSnapshot() {}

    protected GenericTypeSerializerSnapshot(Class<T> typeClass) {
        this.typeClass = checkNotNull(typeClass, "type class can not be NULL");
    }

    /** Create a serializer that is able to serialize the generic type {@code typeClass}. */
    protected abstract TypeSerializer<T> createSerializer(Class<T> typeClass);

    /** Gets the type class from the corresponding serializer. */
    protected abstract Class<T> getTypeClass(S serializer);

    /** Gets the serializer's class. */
    protected abstract Class<?> serializerClass();

    @Override
    public final int getCurrentVersion() {
        return VERSION;
    }

    @Override
    public final void writeSnapshot(DataOutputView out) throws IOException {
        checkState(typeClass != null, "type class can not be NULL");
        out.writeUTF(typeClass.getName());
    }

    @Override
    public final void readSnapshot(
            int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
        typeClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
    }

    @Override
    @SuppressWarnings("unchecked")
    public final TypeSerializer<T> restoreSerializer() {
        checkState(typeClass != null, "type class can not be NULL");
        return createSerializer(typeClass);
    }

    @Override
    public final TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializer<T> newSerializer) {
        if (!serializerClass().isInstance(newSerializer)) {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
        @SuppressWarnings("unchecked")
        S casted = (S) newSerializer;
        if (typeClass == getTypeClass(casted)) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        } else {
            return TypeSerializerSchemaCompatibility.incompatible();
        }
    }
}
