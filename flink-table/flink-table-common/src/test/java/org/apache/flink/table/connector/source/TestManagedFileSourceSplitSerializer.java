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

package org.apache.flink.table.connector.source;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;

/** Managed {@link SimpleVersionedSerializer} for testing. */
public class TestManagedFileSourceSplitSerializer
        implements SimpleVersionedSerializer<TestManagedIterableSourceSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(TestManagedIterableSourceSplit split) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeUTF(split.splitId());
        Path.serializeToDataOutputView(split.getFilePath(), out);
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TestManagedIterableSourceSplit deserialize(int version, byte[] serialized)
            throws IOException {
        if (version == VERSION) {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            final String id = in.readUTF();
            Path result = Path.deserializeFromDataInputView(in);
            final Path path = result == null ? new Path() : result;
            return new TestManagedIterableSourceSplit(id, path);
        }
        throw new IOException(String.format("Unknown version %d", version));
    }
}
