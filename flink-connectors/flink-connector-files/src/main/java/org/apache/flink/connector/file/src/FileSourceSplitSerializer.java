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

package org.apache.flink.connector.file.src;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.file.src.util.CheckpointedPosition;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/** A serializer for the {@link FileSourceSplit}. */
@PublicEvolving
public final class FileSourceSplitSerializer implements SimpleVersionedSerializer<FileSourceSplit> {

    public static final FileSourceSplitSerializer INSTANCE = new FileSourceSplitSerializer();

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private static final int VERSION = 1;

    // ------------------------------------------------------------------------

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(FileSourceSplit split) throws IOException {
        checkArgument(
                split.getClass() == FileSourceSplit.class,
                "Cannot serialize subclasses of FileSourceSplit");

        // optimization: the splits lazily cache their own serialized form
        if (split.serializedFormCache != null) {
            return split.serializedFormCache;
        }

        final DataOutputSerializer out = SERIALIZER_CACHE.get();

        out.writeUTF(split.splitId());
        split.path().write(out);
        out.writeLong(split.offset());
        out.writeLong(split.length());
        writeStringArray(out, split.hostnames());

        final Optional<CheckpointedPosition> readerPosition = split.getReaderPosition();
        out.writeBoolean(readerPosition.isPresent());
        if (readerPosition.isPresent()) {
            out.writeLong(readerPosition.get().getOffset());
            out.writeLong(readerPosition.get().getRecordsAfterOffset());
        }

        final byte[] result = out.getCopyOfBuffer();
        out.clear();

        // optimization: cache the serialized from, so we avoid the byte work during repeated
        // serialization
        split.serializedFormCache = result;

        return result;
    }

    @Override
    public FileSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version == 1) {
            return deserializeV1(serialized);
        }
        throw new IOException("Unknown version: " + version);
    }

    private static FileSourceSplit deserializeV1(byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        final String id = in.readUTF();
        final Path path = new Path();
        path.read(in);
        final long offset = in.readLong();
        final long len = in.readLong();
        final String[] hosts = readStringArray(in);

        final CheckpointedPosition readerPosition =
                in.readBoolean() ? new CheckpointedPosition(in.readLong(), in.readLong()) : null;

        // instantiate a new split and cache the serialized form
        return new FileSourceSplit(id, path, offset, len, hosts, readerPosition, serialized);
    }

    private static void writeStringArray(DataOutputView out, String[] strings) throws IOException {
        out.writeInt(strings.length);
        for (String string : strings) {
            out.writeUTF(string);
        }
    }

    private static String[] readStringArray(DataInputView in) throws IOException {
        final int len = in.readInt();
        final String[] strings = new String[len];
        for (int i = 0; i < len; i++) {
            strings[i] = in.readUTF();
        }
        return strings;
    }
}
