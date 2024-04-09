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
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A serializer for {@link java.util.Set}. The serializer relies on an element serializer for the
 * serialization of the set's elements.
 *
 * <p>The serialization format for the set is as follows: four bytes for the length of the set,
 * followed by the serialized representation of each element. To allow null values, each value is
 * prefixed by a null marker.
 *
 * @param <T> The type of element in the set.
 */
@Internal
public final class SetSerializer<T> extends TypeSerializer<Set<T>> {

    private static final long serialVersionUID = 1L;

    /** The serializer for the elements of the set. */
    private final TypeSerializer<T> elementSerializer;

    /**
     * Creates a set serializer that uses the given serializer to serialize the set's elements.
     *
     * @param elementSerializer The serializer for the elements of the set
     */
    public SetSerializer(TypeSerializer<T> elementSerializer) {
        this.elementSerializer = checkNotNull(elementSerializer);
    }

    // ------------------------------------------------------------------------
    //  SetSerializer specific properties
    // ------------------------------------------------------------------------

    /**
     * Gets the serializer for the elements of the set.
     *
     * @return The serializer for the elements of the set
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
    public TypeSerializer<Set<T>> duplicate() {
        TypeSerializer<T> duplicateElement = elementSerializer.duplicate();
        return duplicateElement == elementSerializer ? this : new SetSerializer<>(duplicateElement);
    }

    @Override
    public Set<T> createInstance() {
        return new HashSet<>(0);
    }

    @Override
    public Set<T> copy(Set<T> from) {
        Set<T> newSet = new HashSet<>(from.size());
        for (T element : from) {
            T newElement = element == null ? null : elementSerializer.copy(element);
            newSet.add(newElement);
        }
        return newSet;
    }

    @Override
    public Set<T> copy(Set<T> from, Set<T> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // var length
    }

    @Override
    public void serialize(Set<T> set, DataOutputView target) throws IOException {
        final int size = set.size();
        target.writeInt(size);
        for (T element : set) {
            if (element == null) {
                target.writeBoolean(true);
            } else {
                target.writeBoolean(false);
                elementSerializer.serialize(element, target);
            }
        }
    }

    @Override
    public Set<T> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();
        final Set<T> set = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            boolean isNull = source.readBoolean();
            T element = isNull ? null : elementSerializer.deserialize(source);
            set.add(element);
        }
        return set;
    }

    @Override
    public Set<T> deserialize(Set<T> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        // copy number of elements
        final int num = source.readInt();
        target.writeInt(num);
        for (int i = 0; i < num; i++) {
            boolean isNull = source.readBoolean();
            target.writeBoolean(isNull);
            if (!isNull) {
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
                        && elementSerializer.equals(((SetSerializer<?>) obj).elementSerializer));
    }

    @Override
    public int hashCode() {
        return elementSerializer.hashCode();
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshot & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<Set<T>> snapshotConfiguration() {
        return new SetSerializerSnapshot<>(this);
    }
}
