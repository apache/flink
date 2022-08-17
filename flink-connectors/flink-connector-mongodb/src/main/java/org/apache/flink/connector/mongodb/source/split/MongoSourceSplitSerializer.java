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

package org.apache.flink.connector.mongodb.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.bson.BsonDocument;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/** The {@link SimpleVersionedSerializer serializer} for {@link MongoSourceSplit}. */
public class MongoSourceSplitSerializer implements SimpleVersionedSerializer<MongoSourceSplit> {

    public static final MongoSourceSplitSerializer INSTANCE = new MongoSourceSplitSerializer();

    // This version should be bumped after modifying the MongoSourceSplit.
    public static final int CURRENT_VERSION = 0;

    private static final int SCAN_SPLIT_FLAG = 1;

    private MongoSourceSplitSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(MongoSourceSplit obj) throws IOException {
        // VERSION 0 serialization
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            serializeMongoSplit(out, obj);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public MongoSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        // VERSION 0 deserialization
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            return deserializeMongoSourceSplit(version, in);
        }
    }

    public void serializeMongoSplit(DataOutputStream out, MongoSourceSplit obj) throws IOException {
        if (obj instanceof MongoScanSourceSplit) {
            MongoScanSourceSplit split = (MongoScanSourceSplit) obj;
            out.writeInt(SCAN_SPLIT_FLAG);
            out.writeUTF(split.splitId());
            out.writeUTF(split.getDatabase());
            out.writeUTF(split.getCollection());
            out.writeUTF(split.getMin().toJson());
            out.writeUTF(split.getMax().toJson());
            out.writeUTF(split.getHint().toJson());
        }
    }

    public MongoSourceSplit deserializeMongoSourceSplit(int version, DataInputStream in)
            throws IOException {
        int splitKind = in.readInt();
        if (splitKind == SCAN_SPLIT_FLAG) {
            return deserializeMongoScanSourceSplit(version, in);
        }
        throw new IOException("Unknown split kind: " + splitKind);
    }

    public MongoScanSourceSplit deserializeMongoScanSourceSplit(int version, DataInputStream in)
            throws IOException {
        switch (version) {
            case 0:
                String splitId = in.readUTF();
                String database = in.readUTF();
                String collection = in.readUTF();
                BsonDocument min = BsonDocument.parse(in.readUTF());
                BsonDocument max = BsonDocument.parse(in.readUTF());
                BsonDocument hint = BsonDocument.parse(in.readUTF());
                return new MongoScanSourceSplit(splitId, database, collection, min, max, hint);
            default:
                throw new IOException("Unknown version: " + version);
        }
    }
}
