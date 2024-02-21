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

package org.apache.flink.table.connector.sink;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.catalog.CatalogPartitionSpec;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.CollectionUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Serializer for {@link TestManagedCommittable} for testing compaction. */
public class TestManagedSinkCommittableSerializer
        implements SimpleVersionedSerializer<TestManagedCommittable> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(TestManagedCommittable committable) throws IOException {
        final DataOutputSerializer out = new DataOutputSerializer(64);
        out.writeInt(committable.getToAdd().size());
        for (Map.Entry<CatalogPartitionSpec, List<RowData>> entry :
                committable.getToAdd().entrySet()) {
            serializePartitionSpec(out, entry.getKey());
            serializeRowDataElements(out, entry.getValue());
        }
        out.writeInt(committable.getToDelete().size());
        for (Map.Entry<CatalogPartitionSpec, Set<Path>> entry :
                committable.getToDelete().entrySet()) {
            serializePartitionSpec(out, entry.getKey());
            serializePaths(out, entry.getValue());
        }
        final byte[] result = out.getCopyOfBuffer();
        out.clear();
        return result;
    }

    @Override
    public TestManagedCommittable deserialize(int version, byte[] serialized) throws IOException {
        if (version == VERSION) {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            int newFileSize = in.readInt();
            Map<CatalogPartitionSpec, List<RowData>> toCommit = new HashMap<>(newFileSize);
            for (int i = 0; i < newFileSize; i++) {
                CatalogPartitionSpec partitionSpec = deserializePartitionSpec(in);
                List<RowData> elements = deserializeRowDataElements(in);
                toCommit.put(partitionSpec, elements);
            }

            int cleanupFileSize = in.readInt();
            Map<CatalogPartitionSpec, Set<Path>> toCleanup = new HashMap<>(cleanupFileSize);
            for (int i = 0; i < cleanupFileSize; i++) {
                CatalogPartitionSpec partitionSpec = deserializePartitionSpec(in);
                Set<Path> paths = deserializePaths(in);
                toCleanup.put(partitionSpec, paths);
            }
            return new TestManagedCommittable(toCommit, toCleanup);
        }
        throw new IOException(String.format("Unknown version %d", version));
    }

    private void serializePartitionSpec(
            DataOutputSerializer out, CatalogPartitionSpec partitionSpec) throws IOException {
        Map<String, String> partitionKVs = partitionSpec.getPartitionSpec();
        out.writeInt(partitionKVs.size());
        for (Map.Entry<String, String> partitionKV : partitionKVs.entrySet()) {
            out.writeUTF(partitionKV.getKey());
            out.writeUTF(partitionKV.getValue());
        }
    }

    private void serializeRowDataElements(DataOutputSerializer out, List<RowData> elements)
            throws IOException {
        out.writeInt(elements.size());
        for (RowData element : elements) {
            out.writeUTF(element.getString(0).toString());
        }
    }

    private void serializePaths(DataOutputSerializer out, Set<Path> paths) throws IOException {
        out.writeInt(paths.size());
        for (Path path : paths) {
            Path.serializeToDataOutputView(path, out);
        }
    }

    private CatalogPartitionSpec deserializePartitionSpec(DataInputDeserializer in)
            throws IOException {
        int size = in.readInt();
        LinkedHashMap<String, String> partitionKVs =
                CollectionUtil.newLinkedHashMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            String partitionKey = in.readUTF();
            String partitionValue = in.readUTF();
            partitionKVs.put(partitionKey, partitionValue);
        }
        return new CatalogPartitionSpec(partitionKVs);
    }

    private List<RowData> deserializeRowDataElements(DataInputDeserializer in) throws IOException {
        int size = in.readInt();
        List<RowData> elements = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            elements.add(GenericRowData.of(StringData.fromString(in.readUTF())));
        }
        return elements;
    }

    private Set<Path> deserializePaths(DataInputDeserializer in) throws IOException {
        int size = in.readInt();
        Set<Path> paths = new HashSet<>(size);
        for (int i = 0; i < size; i++) {
            Path result = Path.deserializeFromDataInputView(in);
            paths.add(result == null ? new Path() : result);
        }
        return paths;
    }
}
