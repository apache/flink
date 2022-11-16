/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/** A simple serializer for versioned serialization of collections. */
@PublicEvolving
public class CollectionSerializer<E> implements SimpleVersionedSerializer<Collection<E>> {
    static final int VERSION = 1;

    private final SimpleObjectSerializer<E> delegate;

    public CollectionSerializer(SimpleObjectSerializer<E> delegate) {
        this.delegate = delegate;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(Collection<E> elements) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            outputStream.writeInt(elements.size());
            for (E element : elements) {
                delegate.serialize(element, outputStream);
            }
        }
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public Collection<E> deserialize(int version, byte[] serialized) throws IOException {
        Preconditions.checkArgument(version == VERSION);
        try (ObjectInputStream inputStream =
                new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            int size = inputStream.readInt();
            List<E> splits = new ArrayList<>();

            while (--size > 0) {
                splits.add(delegate.deserialize(inputStream));
            }
            return splits;
        } catch (ClassNotFoundException exc) {
            throw new IOException("Failed to deserialize FromElementsSplit", exc);
        }
    }
}
