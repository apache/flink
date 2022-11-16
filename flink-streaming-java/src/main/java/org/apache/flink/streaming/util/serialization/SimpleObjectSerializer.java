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

/** A simple serializer for versioned serialization via Object(Output/Input)Stream. */
@PublicEvolving
public abstract class SimpleObjectSerializer<E> implements SimpleVersionedSerializer<E> {

    @Override
    public byte[] serialize(E obj) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            serialize(obj, outputStream);
        }
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public E deserialize(int version, byte[] serialized) throws IOException {
        Preconditions.checkArgument(version == getVersion());
        try (ObjectInputStream inputStream =
                new ObjectInputStream(new ByteArrayInputStream(serialized))) {
            return deserialize(inputStream);
        } catch (ClassNotFoundException exc) {
            throw new IOException("Failed to deserialize FromElementsSplit", exc);
        }
    }

    /** Serializes the given object to the outputStream. */
    protected abstract void serialize(E obj, ObjectOutputStream outputStream) throws IOException;

    /** Deserializes the given object from the inputStream. */
    protected abstract E deserialize(ObjectInputStream inputStream)
            throws IOException, ClassNotFoundException;
}
