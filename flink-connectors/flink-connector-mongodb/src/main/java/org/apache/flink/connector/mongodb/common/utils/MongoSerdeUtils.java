/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.mongodb.common.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionWithException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** A util class with some helper method for serde in the MongoDB source. */
@Internal
public class MongoSerdeUtils {

    /** Private constructor for util class. */
    private MongoSerdeUtils() {}

    public static <T> void serializeList(
            DataOutputStream out,
            List<T> list,
            BiConsumerWithException<DataOutputStream, T, IOException> serializer)
            throws IOException {
        out.writeInt(list.size());
        for (T t : list) {
            serializer.accept(out, t);
        }
    }

    public static <T> List<T> deserializeList(
            DataInputStream in, FunctionWithException<DataInputStream, T, IOException> deserializer)
            throws IOException {
        int size = in.readInt();
        List<T> list = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            T t = deserializer.apply(in);
            list.add(t);
        }

        return list;
    }

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
