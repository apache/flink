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

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.List;

/** Snapshot class for the {@link ListSerializer}. */
public class ListSerializerSnapshot<T>
        extends CompositeTypeSerializerSnapshot<List<T>, ListSerializer<T>> {

    private static final int CURRENT_VERSION = 2;

    private static final int FIRST_VERSION_WITH_NULL_MASK = 2;

    private boolean hasNullMask = true;

    /** Constructor for read instantiation. */
    public ListSerializerSnapshot() {}

    /** Constructor to create the snapshot for writing. */
    public ListSerializerSnapshot(ListSerializer<T> listSerializer) {
        super(listSerializer);
        this.hasNullMask = listSerializer.isHasNullMask();
    }

    @Override
    public int getCurrentOuterSnapshotVersion() {
        return CURRENT_VERSION;
    }

    @Override
    protected void readOuterSnapshot(
            int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader)
            throws IOException {
        if (readOuterSnapshotVersion < FIRST_VERSION_WITH_NULL_MASK) {
            hasNullMask = false;
        } else {
            hasNullMask = in.readBoolean();
        }
    }

    @Override
    protected void writeOuterSnapshot(DataOutputView out) throws IOException {
        out.writeBoolean(hasNullMask);
    }

    @Override
    protected OuterSchemaCompatibility resolveOuterSchemaCompatibility(
            TypeSerializerSnapshot<List<T>> oldSerializerSnapshot) {
        if (!(oldSerializerSnapshot instanceof ListSerializerSnapshot)) {
            return OuterSchemaCompatibility.INCOMPATIBLE;
        }

        ListSerializerSnapshot<T> oldListSerializerSnapshot =
                (ListSerializerSnapshot<T>) oldSerializerSnapshot;
        if (hasNullMask != oldListSerializerSnapshot.hasNullMask) {
            return OuterSchemaCompatibility.COMPATIBLE_AFTER_MIGRATION;
        }
        return OuterSchemaCompatibility.COMPATIBLE_AS_IS;
    }

    @Override
    protected ListSerializer<T> createOuterSerializerWithNestedSerializers(
            TypeSerializer<?>[] nestedSerializers) {
        @SuppressWarnings("unchecked")
        TypeSerializer<T> elementSerializer = (TypeSerializer<T>) nestedSerializers[0];
        return new ListSerializer<>(elementSerializer, hasNullMask);
    }

    @Override
    protected TypeSerializer<?>[] getNestedSerializers(ListSerializer<T> outerSerializer) {
        return new TypeSerializer<?>[] {outerSerializer.getElementSerializer()};
    }

    @SuppressWarnings("unchecked")
    public TypeSerializerSnapshot<T> getElementSerializerSnapshot() {
        return (TypeSerializerSnapshot<T>) getNestedSerializerSnapshots()[0];
    }
}
