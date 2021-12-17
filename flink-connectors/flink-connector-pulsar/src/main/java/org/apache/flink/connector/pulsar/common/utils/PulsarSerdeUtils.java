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

package org.apache.flink.connector.pulsar.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Util for serialize and deserialize. */
@Internal
public final class PulsarSerdeUtils {

    private PulsarSerdeUtils() {
        // No public constructor.
    }

    // Bytes serialization.

    public static void serializeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static byte[] deserializeBytes(DataInputStream in) throws IOException {
        int size = in.readInt();
        byte[] bytes = new byte[size];
        int result = in.read(bytes);
        if (result < 0) {
            throw new IOException("Couldn't deserialize the object, wrong byte buffer.");
        }

        return bytes;
    }

    // Common Object serialization.

    public static void serializeObject(DataOutputStream out, Object obj) throws IOException {
        Preconditions.checkNotNull(obj);

        byte[] objectBytes = InstantiationUtil.serializeObject(obj);
        serializeBytes(out, objectBytes);
    }

    public static <T> T deserializeObject(DataInputStream in) throws IOException {
        byte[] objectBytes = deserializeBytes(in);
        ClassLoader loader = Thread.currentThread().getContextClassLoader();

        try {
            return InstantiationUtil.deserializeObject(objectBytes, loader);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

    // Common Set serialization.

    public static <T> void serializeSet(
            DataOutputStream out,
            Set<T> set,
            BiConsumerWithException<DataOutputStream, T, IOException> serializer)
            throws IOException {
        out.writeInt(set.size());
        for (T t : set) {
            serializer.accept(out, t);
        }
    }

    public static <T> Set<T> deserializeSet(
            DataInputStream in, FunctionWithException<DataInputStream, T, IOException> deserializer)
            throws IOException {
        int size = in.readInt();
        Set<T> set = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            T t = deserializer.apply(in);
            set.add(t);
        }

        return set;
    }

    // Common Map serialization.

    public static <K, V> void serializeMap(
            DataOutputStream out,
            Map<K, V> map,
            BiConsumerWithException<DataOutputStream, K, IOException> keySerializer,
            BiConsumerWithException<DataOutputStream, V, IOException> valueSerializer)
            throws IOException {
        out.writeInt(map.size());
        for (Map.Entry<K, V> entry : map.entrySet()) {
            keySerializer.accept(out, entry.getKey());
            valueSerializer.accept(out, entry.getValue());
        }
    }

    public static <K, V> Map<K, V> deserializeMap(
            DataInputStream in,
            FunctionWithException<DataInputStream, K, IOException> keyDeserializer,
            FunctionWithException<DataInputStream, V, IOException> valueDeserializer)
            throws IOException {
        int size = in.readInt();
        Map<K, V> result = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            K key = keyDeserializer.apply(in);
            V value = valueDeserializer.apply(in);
            result.put(key, value);
        }
        return result;
    }
}
