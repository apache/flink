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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * A utility {@link TypeSerializerConfigSnapshot} that is used for backwards compatibility purposes.
 *
 * <p>In older versions of Flink (<= 1.2), we only wrote serializers and not their corresponding
 * snapshots. This class serves as a wrapper around the restored serializer instances.
 *
 * @param <T> the data type that the wrapped serializer instance serializes.
 */
@Internal
public class BackwardsCompatibleSerializerSnapshot<T> implements TypeSerializerSnapshot<T> {

    /** The serializer instance written in savepoints. */
    @Nonnull private TypeSerializer<T> serializerInstance;

    public BackwardsCompatibleSerializerSnapshot(TypeSerializer<T> serializerInstance) {
        this.serializerInstance = Preconditions.checkNotNull(serializerInstance);
    }

    @Override
    public void writeSnapshot(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException(
                "This is a dummy config snapshot used only for backwards compatibility.");
    }

    @Override
    public void readSnapshot(int version, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        throw new UnsupportedOperationException(
                "This is a dummy config snapshot used only for backwards compatibility.");
    }

    @Override
    public int getCurrentVersion() {
        throw new UnsupportedOperationException(
                "This is a dummy config snapshot used only for backwards compatibility.");
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        return serializerInstance;
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializer<T> newSerializer) {
        // if there is no configuration snapshot to check against,
        // then we can only assume that the new serializer is compatible as is
        return TypeSerializerSchemaCompatibility.compatibleAsIs();
    }

    @Override
    public int hashCode() {
        return serializerInstance.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        BackwardsCompatibleSerializerSnapshot<?> that =
                (BackwardsCompatibleSerializerSnapshot<?>) o;

        return that.serializerInstance.equals(serializerInstance);
    }
}
