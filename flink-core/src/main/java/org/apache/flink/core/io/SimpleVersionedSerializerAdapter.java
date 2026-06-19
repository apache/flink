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

package org.apache.flink.core.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.io.Serializable;

/**
 * Adapter for {@link TypeSerializer} to {@link SimpleVersionedSerializer}. The implementation is
 * naive and should only be used for non-critical paths and tests.
 */
@Internal
public class SimpleVersionedSerializerAdapter<T>
        implements SimpleVersionedSerializer<T>, Serializable {
    private final TypeSerializer<T> serializer;

    public SimpleVersionedSerializerAdapter(TypeSerializer<T> serializer) {
        this.serializer = serializer;
    }

    public int getVersion() {
        return serializer.snapshotConfiguration().getCurrentVersion();
    }

    public byte[] serialize(T value) throws IOException {
        DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(10);
        serializer.serialize(value, dataOutputSerializer);
        return dataOutputSerializer.getCopyOfBuffer();
    }

    public T deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer dataInputDeserializer = new DataInputDeserializer(serialized);
        T value = serializer.deserialize(dataInputDeserializer);
        dataInputDeserializer.releaseArrays();
        return value;
    }
}
