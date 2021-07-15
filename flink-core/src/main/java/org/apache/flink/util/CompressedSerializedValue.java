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

import java.io.IOException;

/** An extension of {@link SerializedValue} that compresses the value after the serialization. */
public class CompressedSerializedValue<T> extends SerializedValue<T> {

    private static final long serialVersionUID = -4358765382738374654L;

    private CompressedSerializedValue(byte[] serializedData) {
        super(serializedData);
    }

    /**
     * Constructs a compressed serialized value.
     *
     * @param value value to serialize and then compress
     * @throws NullPointerException if value is null
     * @throws IOException exception during serialization and compression
     */
    private CompressedSerializedValue(T value) throws IOException {
        super(InstantiationUtil.serializeObjectAndCompress(value));
    }

    @Override
    public T deserializeValue(ClassLoader loader) throws IOException, ClassNotFoundException {
        Preconditions.checkNotNull(loader, "No classloader has been passed");
        return InstantiationUtil.uncompressAndDeserializeObject(getByteArray(), loader);
    }

    /** Returns the size of the compressed serialized data. */
    public int getSize() {
        return getByteArray().length;
    }

    /**
     * Constructs {@link CompressedSerializedValue} from compressed serialized data.
     *
     * @param serializedData compressed serialized data
     * @param <T> type
     * @return {@link CompressedSerializedValue}
     * @throws NullPointerException if serialized data is null
     * @throws IllegalArgumentException if serialized data is empty
     */
    public static <T> CompressedSerializedValue<T> fromBytes(byte[] serializedData) {
        return new CompressedSerializedValue<>(serializedData);
    }

    public static <T> CompressedSerializedValue<T> fromObject(T object) throws IOException {
        Preconditions.checkNotNull(object, "Value must not be null");
        return new CompressedSerializedValue<>(object);
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format("Compressed Serialized Value [byte array length: %d]", getSize());
    }
}
