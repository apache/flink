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

package org.apache.flink.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.BiFunctionWithException;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/** LinkedOptionalMapSerializer - A serializer of {@link LinkedOptionalMap}. */
@Internal
public final class LinkedOptionalMapSerializer {

    /** This header is used for sanity checks on input streams. */
    private static final long HEADER = 0x4f2c69a3d70L;

    private LinkedOptionalMapSerializer() {}

    public static <K, V> void writeOptionalMap(
            DataOutputView out,
            LinkedOptionalMap<K, V> map,
            BiConsumerWithException<DataOutputView, K, IOException> keyWriter,
            BiConsumerWithException<DataOutputView, V, IOException> valueWriter)
            throws IOException {

        out.writeLong(HEADER);
        out.writeInt(map.size());
        map.forEach(
                ((keyName, key, value) -> {
                    out.writeUTF(keyName);

                    if (key == null) {
                        out.writeBoolean(false);
                    } else {
                        out.writeBoolean(true);
                        writeFramed(out, keyWriter, key);
                    }

                    if (value == null) {
                        out.writeBoolean(false);
                    } else {
                        out.writeBoolean(true);
                        writeFramed(out, valueWriter, value);
                    }
                }));
    }

    public static <K, V> LinkedOptionalMap<K, V> readOptionalMap(
            DataInputView in,
            BiFunctionWithException<DataInputView, String, K, IOException> keyReader,
            BiFunctionWithException<DataInputView, String, V, IOException> valueReader)
            throws IOException {

        final long header = in.readLong();
        checkState(header == HEADER, "Corrupted stream received header %s", header);

        long mapSize = in.readInt();
        LinkedOptionalMap<K, V> map = new LinkedOptionalMap<>();
        for (int i = 0; i < mapSize; i++) {
            String keyName = in.readUTF();

            final K key;
            if (in.readBoolean()) {
                key = tryReadFrame(in, keyName, keyReader);
            } else {
                key = null;
            }

            final V value;
            if (in.readBoolean()) {
                value = tryReadFrame(in, keyName, valueReader);
            } else {
                value = null;
            }

            map.put(keyName, key, value);
        }
        return map;
    }

    private static <T> void writeFramed(
            DataOutputView out,
            BiConsumerWithException<DataOutputView, T, IOException> writer,
            T item)
            throws IOException {
        DataOutputSerializer frame = new DataOutputSerializer(64);
        writer.accept(frame, item);

        final byte[] buffer = frame.getSharedBuffer();
        final int bufferSize = frame.length();
        out.writeInt(bufferSize);
        out.write(buffer, 0, bufferSize);
    }

    @Nullable
    private static <T> T tryReadFrame(
            DataInputView in,
            String keyName,
            BiFunctionWithException<DataInputView, String, T, IOException> reader)
            throws IOException {
        final int bufferSize = in.readInt();
        final byte[] buffer = new byte[bufferSize];
        in.readFully(buffer);
        DataInputDeserializer frame = new DataInputDeserializer(buffer);
        return reader.apply(frame, keyName);
    }
}
