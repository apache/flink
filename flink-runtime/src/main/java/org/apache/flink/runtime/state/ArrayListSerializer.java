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
package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.CollectionSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.ArrayList;

@SuppressWarnings("ForLoopReplaceableByForEach")
public final class ArrayListSerializer<T> extends TypeSerializer<ArrayList<T>>
        implements TypeSerializerConfigSnapshot.SelfResolvingTypeSerializer<ArrayList<T>> {

    private static final long serialVersionUID = 1119562170939152304L;

    private final TypeSerializer<T> elementSerializer;

    public ArrayListSerializer(TypeSerializer<T> elementSerializer) {
        this.elementSerializer = elementSerializer;
    }

    public TypeSerializer<T> getElementSerializer() {
        return elementSerializer;
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<ArrayList<T>> duplicate() {
        TypeSerializer<T> duplicateElement = elementSerializer.duplicate();
        return duplicateElement == elementSerializer
                ? this
                : new ArrayListSerializer<T>(duplicateElement);
    }

    @Override
    public ArrayList<T> createInstance() {
        return new ArrayList<>();
    }

    @Override
    public ArrayList<T> copy(ArrayList<T> from) {
        if (elementSerializer.isImmutableType()) {
            // fast track using memcopy for immutable types
            return new ArrayList<>(from);
        } else {
            // element-wise deep copy for mutable types
            ArrayList<T> newList = new ArrayList<>(from.size());
            for (int i = 0; i < from.size(); i++) {
                newList.add(elementSerializer.copy(from.get(i)));
            }
            return newList;
        }
    }

    @Override
    public ArrayList<T> copy(ArrayList<T> from, ArrayList<T> reuse) {
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1; // var length
    }

    @Override
    public void serialize(ArrayList<T> list, DataOutputView target) throws IOException {
        final int size = list.size();
        target.writeInt(size);
        for (int i = 0; i < size; i++) {
            elementSerializer.serialize(list.get(i), target);
        }
    }

    @Override
    public ArrayList<T> deserialize(DataInputView source) throws IOException {
        final int size = source.readInt();
        final ArrayList<T> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            list.add(elementSerializer.deserialize(source));
        }
        return list;
    }

    @Override
    public ArrayList<T> deserialize(ArrayList<T> reuse, DataInputView source) throws IOException {
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        // copy number of elements
        final int num = source.readInt();
        target.writeInt(num);
        for (int i = 0; i < num; i++) {
            elementSerializer.copy(source, target);
        }
    }

    // --------------------------------------------------------------------

    @Override
    public boolean equals(Object obj) {
        return obj == this
                || (obj != null
                        && obj.getClass() == getClass()
                        && elementSerializer.equals(
                                ((ArrayListSerializer<?>) obj).elementSerializer));
    }

    @Override
    public int hashCode() {
        return elementSerializer.hashCode();
    }

    // --------------------------------------------------------------------------------------------
    // Serializer snapshots
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<ArrayList<T>> snapshotConfiguration() {
        return new ArrayListSerializerSnapshot<>(this);
    }

    /**
     * We need to implement this method as a {@link
     * TypeSerializerConfigSnapshot.SelfResolvingTypeSerializer} because this serializer was
     * previously returning a shared {@link CollectionSerializerConfigSnapshot} as its snapshot.
     *
     * <p>When the {@link CollectionSerializerConfigSnapshot} is restored, it is incapable of
     * redirecting the compatibility check to {@link ArrayListSerializerSnapshot}, so we do it here.
     */
    @Override
    public TypeSerializerSchemaCompatibility<ArrayList<T>>
            resolveSchemaCompatibilityViaRedirectingToNewSnapshotClass(
                    TypeSerializerConfigSnapshot<ArrayList<T>> deprecatedConfigSnapshot) {

        if (deprecatedConfigSnapshot instanceof CollectionSerializerConfigSnapshot) {
            CollectionSerializerConfigSnapshot<ArrayList<T>, T> castedLegacySnapshot =
                    (CollectionSerializerConfigSnapshot<ArrayList<T>, T>) deprecatedConfigSnapshot;

            ArrayListSerializerSnapshot<T> newSnapshot = new ArrayListSerializerSnapshot<>();
            return CompositeTypeSerializerUtil.delegateCompatibilityCheckToNewSnapshot(
                    this, newSnapshot, castedLegacySnapshot.getNestedSerializerSnapshots());
        }

        return TypeSerializerSchemaCompatibility.incompatible();
    }
}
