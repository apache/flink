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
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.PojoSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import javax.annotation.Nullable;

/**
 * A {@link TypeSerializerSnapshot} for deserializers that can read POJO binary data without the
 * user POJO class being on the classpath, such as {@link PojoToRowDataDeserializer}. It declares
 * itself {@link TypeSerializerSchemaCompatibility#compatibleAsIs() compatible as-is} with any
 * stored {@link PojoSerializerSnapshot}.
 *
 * <p>The snapshot only ever exists in memory, wrapping the live deserializer it was created from:
 * composite compatibility checks (e.g. {@code
 * CompositeTypeSerializerSnapshot#resolveOuterSchemaCompatibility}) restore the "new" side of a
 * composite serializer even when nested-level compatibility already short-circuited to {@code
 * compatibleAsIs()}, so {@link #restoreSerializer()} hands back the wrapped instance rather than
 * reconstructing one from persisted bytes.
 */
@Internal
public final class PojoDeserializerCompatibilitySnapshot<T> implements TypeSerializerSnapshot<T> {

    @Nullable private final TypeSerializer<T> restoredSerializer;

    /** Constructor for reading the snapshot; see {@link #restoreSerializer()}. */
    public PojoDeserializerCompatibilitySnapshot() {
        this(null);
    }

    public PojoDeserializerCompatibilitySnapshot(TypeSerializer<T> restoredSerializer) {
        this.restoredSerializer = restoredSerializer;
    }

    @Override
    public int getCurrentVersion() {
        return 1;
    }

    @Override
    public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
            TypeSerializerSnapshot<T> oldSerializerSnapshot) {
        if (oldSerializerSnapshot instanceof PojoSerializerSnapshot
                || oldSerializerSnapshot instanceof PojoDeserializerCompatibilitySnapshot) {
            return TypeSerializerSchemaCompatibility.compatibleAsIs();
        }
        return TypeSerializerSchemaCompatibility.incompatible();
    }

    @Override
    public TypeSerializer<T> restoreSerializer() {
        if (restoredSerializer == null) {
            throw new UnsupportedOperationException(
                    "PojoDeserializerCompatibilitySnapshot cannot reconstruct the deserializer on "
                            + "its own. Use PojoSerializerSnapshot.restoreSerializer() or "
                            + "PojoToRowDataDeserializer.create().");
        }
        return restoredSerializer;
    }

    @Override
    public void writeSnapshot(DataOutputView out) {}

    @Override
    public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) {}
}
