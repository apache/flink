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

package org.apache.flink.connector.upserttest.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.util.CollectionUtil;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Collection uf utility methods for reading and writing files from the {@link UpsertTestSink}. */
@Internal
public class UpsertTestFileUtil {
    static final byte MAGIC_BYTE = 13;

    private UpsertTestFileUtil() {}

    /**
     * Writes a Map of records serialized by the {@link UpsertTestSinkWriter} to the given
     * BufferedOutputStream.
     *
     * @param bos the BufferedOutputStream to write to
     * @param records the Map of records created by the UpsertTestSinkWriter
     * @throws IOException
     */
    public static void writeRecords(
            BufferedOutputStream bos,
            Map<ImmutableByteArrayWrapper, ImmutableByteArrayWrapper> records)
            throws IOException {
        checkNotNull(bos);
        for (Map.Entry<ImmutableByteArrayWrapper, ImmutableByteArrayWrapper> record :
                records.entrySet()) {
            byte[] key = record.getKey().array();
            byte[] value = record.getValue().array();
            bos.write(MAGIC_BYTE);
            bos.write(key.length);
            bos.write(key);
            bos.write(value.length);
            bos.write(value);
        }
        bos.flush();
    }

    /**
     * Returns the total number of records written using the {@link UpsertTestSinkWriter} to the
     * given File.
     *
     * @param bis The BufferedInputStream to read from
     * @return the number of records
     * @throws IOException
     */
    public static int getNumberOfRecords(BufferedInputStream bis) throws IOException {
        Map<ImmutableByteArrayWrapper, ImmutableByteArrayWrapper> records = readRecords(bis);
        return records.size();
    }

    /**
     * Returns the total number of records written using the {@link UpsertTestSinkWriter} to the
     * given File.
     *
     * @param file The File to read from
     * @return the number of records
     * @throws IOException
     */
    public static int getNumberOfRecords(File file) throws IOException {
        checkNotNull(file);
        FileInputStream fs = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fs);
        return getNumberOfRecords(bis);
    }

    /**
     * Reads records that were written using the {@link UpsertTestSinkWriter} from the given
     * InputStream and converts them using the provided {@link DeserializationSchema}s.
     *
     * @param bis The BufferedInputStream to read from
     * @param keyDeserializationSchema The key's DeserializationSchema
     * @param valueDeserializationSchema The value's DeserializationSchema
     * @return Map containing the deserialized key-value pairs
     * @throws IOException
     */
    public static <K, V> Map<K, V> readRecords(
            BufferedInputStream bis,
            DeserializationSchema<K> keyDeserializationSchema,
            DeserializationSchema<V> valueDeserializationSchema)
            throws IOException {
        checkNotNull(bis);
        Map<ImmutableByteArrayWrapper, ImmutableByteArrayWrapper> bytesMap = readRecords(bis);
        Map<K, V> typedMap = CollectionUtil.newHashMapWithExpectedSize(bytesMap.size());

        Iterator<Map.Entry<ImmutableByteArrayWrapper, ImmutableByteArrayWrapper>> it =
                bytesMap.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<ImmutableByteArrayWrapper, ImmutableByteArrayWrapper> entry = it.next();
            K key = keyDeserializationSchema.deserialize(entry.getKey().array());
            V value = valueDeserializationSchema.deserialize(entry.getValue().array());
            typedMap.put(key, value);
        }
        return typedMap;
    }

    /**
     * Reads records that were written using the {@link UpsertTestSinkWriter} from the given File
     * and converts them using the provided {@link DeserializationSchema}s.
     *
     * @param file The File to read from
     * @param keyDeserializationSchema The key's DeserializationSchema
     * @param valueDeserializationSchema The value's DeserializationSchema
     * @return Map containing the deserialized key-value pairs
     * @throws IOException
     */
    public static <K, V> Map<K, V> readRecords(
            File file,
            DeserializationSchema<K> keyDeserializationSchema,
            DeserializationSchema<V> valueDeserializationSchema)
            throws IOException {
        checkNotNull(file);
        FileInputStream fs = new FileInputStream(file);
        BufferedInputStream bis = new BufferedInputStream(fs);
        return readRecords(bis, keyDeserializationSchema, valueDeserializationSchema);
    }

    /**
     * Reads records that were written using the {@link UpsertTestSinkWriter} from the given
     * InputStream.
     *
     * @param bis The BufferedInputStream to read from
     * @return Map containing the read ImmutableByteArrayWrapper key-value pairs
     * @throws IOException
     */
    private static Map<ImmutableByteArrayWrapper, ImmutableByteArrayWrapper> readRecords(
            BufferedInputStream bis) throws IOException {
        checkNotNull(bis);
        Map<ImmutableByteArrayWrapper, ImmutableByteArrayWrapper> records = new HashMap<>();
        int magicByte;
        while ((magicByte = bis.read()) != -1) {
            if (magicByte != MAGIC_BYTE) {
                throw new IOException("Data was serialized incorrectly or is corrupted.");
            }
            int keyLength = bis.read();
            byte[] key = new byte[keyLength];
            bis.read(key);
            int valueLength = bis.read();
            byte[] value = new byte[valueLength];
            bis.read(value);
            records.put(new ImmutableByteArrayWrapper(key), new ImmutableByteArrayWrapper(value));
        }
        return records;
    }
}
