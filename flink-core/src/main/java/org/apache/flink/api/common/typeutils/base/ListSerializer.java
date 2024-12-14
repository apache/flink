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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.MaskUtils;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer for {@link List Lists}. The serializer relies on an element serializer for the
 * serialization of the list's elements.
 *
 * <p>The serialization format for the list is as follows: four bytes for the length of the list, an
 * optional binary mask for marking null values, followed by the serialized representation of each
 * element. The binary mask is added in the new version to allow null values, and we rely on
 * TypeSerializerSnapshot to deal with state-compatibility.
 *
 * @param <T> The type of element in the list.
 */
@Internal
public final class ListSerializer<T> extends TypeSerializer<List<T>> {

    private static final long serialVersionUID = 1119562170939152304L;

    /** The serializer for the elements of the list. */
    private final TypeSerializer<T> elementSerializer;

    private final boolean hasNullMask;
    private transient boolean[] reuseMask;

    /**
     * Creates a list serializer that uses the given serializer to serialize the list's elements.
     *
     * @param elementSerializer The serializer for the elements of the list
     */
    public ListSerializer(TypeSerializer<T> elementSerializer) {
        this(elementSerializer, true);
    }

    public ListSerializer(TypeSerializer<T> elementSerializer, boolean hasNullMask) {
        this.elementSerializer = checkNotNull(elementSerializer);
        this.hasNullMask = hasNullMask;
    }

    // ------------------------------------------------------------------------
    //  ListSerializer specific properties
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
    public TypeSerializer<List<T>> duplicate() {
        return new ListSerializer<>(elementSerializer.duplicate(), hasNullMask);
    }

    @Override
    public List<T> createInstance() {
        return new ArrayList<>(0);
    }

    @Override
    public List<T> copy(List<T> from) {
        List<T> newList = new ArrayList<>(from.size());

        // We iterate here rather than accessing by index, because we cannot be sure that
        // the given list supports RandomAccess.
        // The Iterator should be stack allocated on new JVMs (due to escape analysis)
        for (T element : from) {
            T newElement = element == null ? null : elementSerializer.copy(element);
            newList.add(newElement);
        }
        return newList;
    }

    @Override
    public List<T> copy(List<T> from, List<T> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // var length
    }

    @Override
    public void serialize(List<T> list, DataOutputView target) throws IOException {
        final int size = list.size();
        target.writeInt(size);

        if (hasNullMask) {
            ensureReuseMaskLength(list.size());
            MaskUtils.writeMask(getNullMask(list), list.size(), target);
        }

        // We iterate here rather than accessing by index, because we cannot be sure that
        // the given list supports RandomAccess.
        // The Iterator should be stack allocated on new JVMs (due to escape analysis)
        for (T element : list) {
            if (element != null) {
                elementSerializer.serialize(element, target);
            }
        }
    }

    @Override
    public List<T> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();
        // create new list with (size + 1) capacity to prevent expensive growth when a single
        // element is added
        final List<T> list = new ArrayList<>(size + 1);
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
    public List<T> deserialize(List<T> reuse, DataInputView source) throws IOException {
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

    private void ensureReuseMaskLength(int len) {
        if (reuseMask == null || reuseMask.length < len) {
            reuseMask = new boolean[len];
        }
    }

    private boolean[] getNullMask(List<T> list) {
        int idx = 0;
        for (T item : list) {
            reuseMask[idx] = item == null;
            idx++;
        }
        return reuseMask;
    }

    // --------------------------------------------------------------------

    public boolean isHasNullMask() {
        return hasNullMask;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && elementSerializer.equals(((ListSerializer<?>) obj).elementSerializer)
                        && hasNullMask == ((ListSerializer<?>) obj).hasNullMask);
    }

    @Override
    public int hashCode() {
        return Objects.hash(elementSerializer, hasNullMask);
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshot & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<List<T>> snapshotConfiguration() {
        return new ListSerializerSnapshot<>(this);
    }
}
