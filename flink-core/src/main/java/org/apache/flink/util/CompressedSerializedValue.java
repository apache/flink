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
 * limitations under the License
 */

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;

import java.io.IOException;

/** An extension of {@link SerializedValue} that compresses the value after the serialization. */
@Internal
public class CompressedSerializedValue<T> extends SerializedValue<T> {

    private static final long serialVersionUID = -4358765382738374654L;

    private CompressedSerializedValue(byte[] compressedSerializedData) {
        super(compressedSerializedData);
    }

    private CompressedSerializedValue(T value) throws IOException {
        super(InstantiationUtil.serializeObjectAndCompress(value));
    }

    /**
     * Decompress and deserialize the data to get the original object.
     *
     * @param loader the classloader to deserialize
     * @return the deserialized object
     * @throws IOException exception during decompression and deserialization
     * @throws ClassNotFoundException if class is not found in the classloader
     */
    @Override
    public T deserializeValue(ClassLoader loader) throws IOException, ClassNotFoundException {
        Preconditions.checkNotNull(loader, "No classloader has been passed");
        return InstantiationUtil.decompressAndDeserializeObject(getByteArray(), loader);
    }

    /** Returns the size of the compressed serialized data. */
    public int getSize() {
        return getByteArray().length;
    }

    /**
     * Constructs a compressed serialized value for the given object.
     *
     * @param object the object to serialize and compress
     * @throws NullPointerException if object is null
     * @throws IOException exception during serialization and compression
     */
    public static <T> CompressedSerializedValue<T> fromObject(T object) throws IOException {
        Preconditions.checkNotNull(object, "Value must not be null");
        return new CompressedSerializedValue<>(object);
    }

    /**
     * Construct a compressed serialized value with a serialized byte array.
     *
     * <p>The byte array must be the result of serialization and compression with {@link
     * InstantiationUtil#serializeObjectAndCompress}.
     *
     * @param compressedSerializedData the compressed serialized byte array
     * @param <T> type of the object
     * @return {@link CompressedSerializedValue} that can be deserialized as the object
     */
    public static <T> CompressedSerializedValue<T> fromBytes(byte[] compressedSerializedData) {
        return new CompressedSerializedValue<>(compressedSerializedData);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format("Compressed Serialized Value [byte array length: %d]", getSize());
    }
}
