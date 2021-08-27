/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.SerializableSupplier;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link TypeSerializer} that delegates to an underlying {@link SimpleVersionedSerializer}.
 *
 * <p>This should not be used as a general {@link TypeSerializer}. It's meant to be used by internal
 * operators that need to work with both {@link SimpleVersionedSerializer} and {@link
 * TypeSerializer}.
 */
@Internal
public class SimpleVersionedSerializerTypeSerializerProxy<T> extends TypeSerializer<T> {

    private final SerializableSupplier<SimpleVersionedSerializer<T>> serializerSupplier;
    private transient SimpleVersionedSerializer<T> cachedSerializer;

    public SimpleVersionedSerializerTypeSerializerProxy(
            SerializableSupplier<SimpleVersionedSerializer<T>> serializerSupplier) {
        this.serializerSupplier = checkNotNull(serializerSupplier, "serializerSupplier");
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public TypeSerializer<T> duplicate() {
        try {
            return new SimpleVersionedSerializerTypeSerializerProxy<>(
                    InstantiationUtil.clone(
                            serializerSupplier, serializerSupplier.getClass().getClassLoader()));
        } catch (ClassNotFoundException | IOException e) {
            throw new RuntimeException("Could not duplicate SimpleVersionedSerializer.", e);
        }
    }

    @Override
    public T createInstance() {
        return null;
    }

    @Override
    public T copy(T from) {
        SimpleVersionedSerializer<T> serializer = getSerializer();
        try {
            byte[] serializedFrom = serializer.serialize(from);
            return serializer.deserialize(serializer.getVersion(), serializedFrom);
        } catch (IOException e) {
            throw new RuntimeException("Could not copy element.", e);
        }
    }

    @Override
    public T copy(T from, T reuse) {
        // the reuse is optional, we can just ignore it
        return copy(from);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        SimpleVersionedSerializer<T> serializer = getSerializer();
        SimpleVersionedSerialization.writeVersionAndSerialize(serializer, record, target);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        SimpleVersionedSerializer<T> serializer = getSerializer();
        return SimpleVersionedSerialization.readVersionAndDeSerialize(serializer, source);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        // the reuse is optional, we can just ignore it
        return deserialize(source);
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        T record = deserialize(source);
        serialize(record, target);
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof SimpleVersionedSerializerTypeSerializerProxy
                && ((SimpleVersionedSerializerTypeSerializerProxy<?>) other)
                        .serializerSupplier
                        .get()
                        .equals(serializerSupplier.get());
    }

    @Override
    public int hashCode() {
        return serializerSupplier.get().hashCode();
    }

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        throw new UnsupportedOperationException(
                "SimpleVersionedSerializerWrapper is not meant to be used as a general TypeSerializer for state.");
    }

    private SimpleVersionedSerializer<T> getSerializer() {
        if (cachedSerializer != null) {
            return cachedSerializer;
        }
        cachedSerializer = serializerSupplier.get();
        return cachedSerializer;
    }
}
