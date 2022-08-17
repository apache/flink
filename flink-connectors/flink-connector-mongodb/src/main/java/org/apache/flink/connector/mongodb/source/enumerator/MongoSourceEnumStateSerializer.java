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

package org.apache.flink.connector.mongodb.source.enumerator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.mongodb.source.split.MongoScanSourceSplit;
import org.apache.flink.connector.mongodb.source.split.MongoSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.apache.flink.connector.mongodb.common.utils.MongoSerdeUtils.deserializeList;
import static org.apache.flink.connector.mongodb.common.utils.MongoSerdeUtils.deserializeMap;
import static org.apache.flink.connector.mongodb.common.utils.MongoSerdeUtils.serializeList;
import static org.apache.flink.connector.mongodb.common.utils.MongoSerdeUtils.serializeMap;

/** The {@link SimpleVersionedSerializer Serializer} for the enumerator state of Mongo source. */
@Internal
public class MongoSourceEnumStateSerializer
        implements SimpleVersionedSerializer<MongoSourceEnumState> {

    public static final MongoSourceEnumStateSerializer INSTANCE =
            new MongoSourceEnumStateSerializer();

    private MongoSourceEnumStateSerializer() {
        // Singleton instance.
    }

    @Override
    public int getVersion() {
        // We use MongoSourceSplitSerializer's version because we use reuse this class.
        return MongoSourceSplitSerializer.CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(MongoSourceEnumState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            serializeList(out, state.getRemainingCollections(), DataOutputStream::writeUTF);

            serializeList(out, state.getAlreadyProcessedCollections(), DataOutputStream::writeUTF);

            serializeList(
                    out,
                    state.getRemainingScanSplits(),
                    MongoSourceSplitSerializer.INSTANCE::serializeMongoSplit);

            serializeMap(
                    out,
                    state.getAssignedScanSplits(),
                    DataOutputStream::writeUTF,
                    MongoSourceSplitSerializer.INSTANCE::serializeMongoSplit);

            out.writeBoolean(state.isInitialized());

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public MongoSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            List<String> remainingCollections = deserializeList(in, DataInput::readUTF);
            List<String> alreadyProcessedCollections = deserializeList(in, DataInput::readUTF);
            List<MongoScanSourceSplit> remainingScanSplits =
                    deserializeList(in, i -> deserializeMongoScanSourceSplit(version, i));

            Map<String, MongoScanSourceSplit> assignedScanSplits =
                    deserializeMap(
                            in,
                            DataInput::readUTF,
                            i -> deserializeMongoScanSourceSplit(version, i));

            boolean initialized = in.readBoolean();

            return new MongoSourceEnumState(
                    remainingCollections,
                    alreadyProcessedCollections,
                    remainingScanSplits,
                    assignedScanSplits,
                    initialized);
        }
    }

    private MongoScanSourceSplit deserializeMongoScanSourceSplit(int version, DataInputStream in)
            throws IOException {
        return (MongoScanSourceSplit)
                MongoSourceSplitSerializer.INSTANCE.deserializeMongoSourceSplit(version, in);
    }
}
