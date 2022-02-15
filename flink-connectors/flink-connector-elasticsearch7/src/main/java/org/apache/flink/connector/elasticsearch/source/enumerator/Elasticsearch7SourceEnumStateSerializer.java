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

package org.apache.flink.connector.elasticsearch.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.elasticsearch.source.split.Elasticsearch7Split;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer Serializer} for the enumerator
 * state of the Elasticsearch source.
 */
@Internal
public class Elasticsearch7SourceEnumStateSerializer
        implements SimpleVersionedSerializer<Elasticsearch7SourceEnumState> {
    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(Elasticsearch7SourceEnumState obj) throws IOException {
        Set<Elasticsearch7Split> assignedSplits = obj.getAssignedSplits();
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            out.writeInt(assignedSplits.size());
            for (Elasticsearch7Split split : assignedSplits) {
                out.writeUTF(split.getPitId());
                out.writeInt(split.getSliceId());
            }
            out.flush();

            return baos.toByteArray();
        }
    }

    @Override
    public Elasticsearch7SourceEnumState deserialize(int version, byte[] serialized)
            throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            final int numSplits = in.readInt();
            final Set<Elasticsearch7Split> splits = new HashSet<>(numSplits);
            for (int i = 0; i < numSplits; i++) {
                final String pitId = in.readUTF();
                final int sliceId = in.readInt();
                splits.add(new Elasticsearch7Split(pitId, sliceId));
            }
            if (in.available() > 0) {
                throw new IOException("Unexpected trailing bytes in serialized topic partitions");
            }

            return new Elasticsearch7SourceEnumState(splits);
        }
    }
}
