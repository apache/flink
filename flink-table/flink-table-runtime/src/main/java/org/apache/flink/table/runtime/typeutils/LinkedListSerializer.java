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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.MaskUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.LinkedList;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer for {@link LinkedList}. The serializer relies on an element serializer for the
 * serialization of the list's elements.
 *
 * @param <T> The type of element in the list.
 */
@Internal
public final class LinkedListSerializer<T> extends TypeSerializer<LinkedList<T>> {

    // legacy, don't touch until we drop support for 1.9 savepoints
    private static final long serialVersionUID = 1L;

    // The serializer for the elements of the list.
    private final TypeSerializer<T> elementSerializer;

    private final boolean hasNullMask;
    private transient boolean[] reuseMask;

    /**
     * Creates a list serializer that uses the given serializer to serialize the list's elements.
     *
     * @param elementSerializer The serializer for the elements of the list
     */
    public LinkedListSerializer(TypeSerializer<T> elementSerializer) {
        this(elementSerializer, true);
    }

    public LinkedListSerializer(TypeSerializer<T> elementSerializer, boolean hasNullMask) {
        this.elementSerializer = checkNotNull(elementSerializer);
        this.hasNullMask = hasNullMask;
    }

    // ------------------------------------------------------------------------
    //  LinkedListSerializer specific properties
    // ------------------------------------------------------------------------

    /**
     * Gets the serializer for the elements of the list.
     *
     * @return The serializer for the elements of the list
     */
    public TypeSerializer<T> getElementSerializer() {
        return elementSerializer;
    }

    // ------------------------------------------------------------------------
    //  Type Serializer implementation
    // ------------------------------------------------------------------------

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<LinkedList<T>> duplicate() {
        return new LinkedListSerializer<>(elementSerializer.duplicate(), hasNullMask);
    }

    @Override
    public LinkedList<T> createInstance() {
        return new LinkedList<>();
    }

    @Override
    public LinkedList<T> copy(LinkedList<T> from) {
        LinkedList<T> newList = new LinkedList<>();
        for (T element : from) {
            // there is no compatibility problem here as it only copies from memory to memory
            if (element == null) {
                newList.add(null);
            } else {
                newList.add(elementSerializer.copy(element));
            }
        }
        return newList;
    }

    @Override
    public LinkedList<T> copy(LinkedList<T> from, LinkedList<T> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // var length
    }

    private void ensureReuseMaskLength(int len) {
        if (reuseMask == null || reuseMask.length < len) {
            reuseMask = new boolean[len];
        }
    }

    @Override
    public void serialize(LinkedList<T> list, DataOutputView target) throws IOException {
        target.writeInt(list.size());
        if (hasNullMask) {
            ensureReuseMaskLength(list.size());
            MaskUtils.writeMask(getNullMask(list), list.size(), target);
        }
        for (T element : list) {
            if (element != null) {
                elementSerializer.serialize(element, target);
            }
        }
    }

    private boolean[] getNullMask(LinkedList<T> list) {
        int idx = 0;
        for (T item : list) {
            reuseMask[idx] = item == null;
            idx++;
        }
        return reuseMask;
    }

    @Override
    public LinkedList<T> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();
        final LinkedList<T> list = new LinkedList<>();
        if (hasNullMask) {
            ensureReuseMaskLength(size);
            MaskUtils.readIntoMask(source, reuseMask, size);
        }
        for (int i = 0; i < size; i++) {
            if (hasNullMask && reuseMask[i]) {
                list.add(null);
            } else {
                list.add(elementSerializer.deserialize(source));
            }
        }
        return list;
    }

    @Override
    public LinkedList<T> deserialize(LinkedList<T> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        // copy number of elements
        final int num = source.readInt();
        target.writeInt(num);
        if (hasNullMask) {
            ensureReuseMaskLength(num);
            MaskUtils.readIntoAndCopyMask(source, target, reuseMask, num);
        }
        for (int i = 0; i < num; i++) {
            if (!(hasNullMask && reuseMask[i])) {
                elementSerializer.copy(source, target);
            }
        }
    }

    // --------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && elementSerializer.equals(
                                ((LinkedListSerializer<?>) obj).elementSerializer));
    }

    @Override
    public int hashCode() {
        return elementSerializer.hashCode();
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshot & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<LinkedList<T>> snapshotConfiguration() {
        return new LinkedListSerializerSnapshot<>(this);
    }

    /** Snapshot class for the {@link LinkedListSerializer}. */
    public static class LinkedListSerializerSnapshot<T>
            extends CompositeTypeSerializerSnapshot<LinkedList<T>, LinkedListSerializer<T>> {

        private static final int CURRENT_VERSION = 3;

        private static final int FIRST_VERSION_WITH_NULL_MASK = 2;

        private boolean hasNullMask = true;

        /** Constructor for read instantiation. */
        public LinkedListSerializerSnapshot() {
            super(LinkedListSerializer.class);
        }

        /** Constructor to create the snapshot for writing. */
        public LinkedListSerializerSnapshot(LinkedListSerializer<T> listSerializer) {
            super(listSerializer);
            this.hasNullMask = listSerializer.hasNullMask;
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
            } else if (readOuterSnapshotVersion == FIRST_VERSION_WITH_NULL_MASK) {
                hasNullMask = true;
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
                LinkedListSerializer<T> newSerializer) {
            if (hasNullMask != newSerializer.hasNullMask) {
                return OuterSchemaCompatibility.COMPATIBLE_AFTER_MIGRATION;
            }
            return OuterSchemaCompatibility.COMPATIBLE_AS_IS;
        }

        @Override
        protected LinkedListSerializer<T> createOuterSerializerWithNestedSerializers(
                TypeSerializer<?>[] nestedSerializers) {
            @SuppressWarnings("unchecked")
            TypeSerializer<T> elementSerializer = (TypeSerializer<T>) nestedSerializers[0];
            return new LinkedListSerializer<>(elementSerializer, hasNullMask);
        }

        @Override
        protected TypeSerializer<?>[] getNestedSerializers(
                LinkedListSerializer<T> outerSerializer) {
            return new TypeSerializer<?>[] {outerSerializer.getElementSerializer()};
        }
    }
}
