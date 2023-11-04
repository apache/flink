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

package org.apache.flink.connector.source.split;

import org.apache.flink.connector.source.DynamicFilteringValuesSource;
import org.apache.flink.connector.source.TerminatingLogic;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** The split serializer for the {@link DynamicFilteringValuesSource}. */
public class ValuesSourcePartitionSplitSerializer
        implements SimpleVersionedSerializer<ValuesSourcePartitionSplit> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(ValuesSourcePartitionSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            Map<String, String> partition = split.getPartition();
            out.writeUTF(split.splitId());
            out.writeInt(partition.size());
            for (Map.Entry<String, String> entry : partition.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
            TerminatingLogic.writeTo(out, split.getTerminatingLogic());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public ValuesSourcePartitionSplit deserialize(int version, byte[] serialized)
            throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            Map<String, String> partition = new HashMap<>();
            String splitId = in.readUTF();
            int size = in.readInt();
            for (int i = 0; i < size; ++i) {
                String key = in.readUTF();
                String value = in.readUTF();
                partition.put(key, value);
            }
            final TerminatingLogic terminatingLogic = TerminatingLogic.readFrom(in);
            ValuesSourcePartitionSplit split =
                    new ValuesSourcePartitionSplit(partition, terminatingLogic);
            Preconditions.checkArgument(split.splitId().equals(splitId));
            return split;
        }
    }
}
