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

package org.apache.flink.connector.hbase.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;

/** A serializer for {@link HBaseSourceSplit}. */
@Internal
public class HBaseSourceSplitSerializer implements SimpleVersionedSerializer<HBaseSourceSplit> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSourceSplitSerializer.class);

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(HBaseSourceSplit split) throws IOException {
        LOG.debug("serializing split {}", split.splitId());
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            out.writeUTF(split.splitId());
            out.writeUTF(split.getHost());
            out.writeInt(split.getColumnFamilies().size());
            for (String columnFamily : split.getColumnFamilies()) {
                out.writeUTF(columnFamily);
            }
            out.writeLong(split.getFirstEventStamp().f0);
            out.writeInt(split.getFirstEventStamp().f1);
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public HBaseSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        LOG.debug("deserializing split");
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            String id = in.readUTF();
            String host = in.readUTF();
            ArrayList<String> columnFamilies = new ArrayList<>();
            int numberOfColumnFamilies = in.readInt();
            for (int i = 0; i < numberOfColumnFamilies; i++) {
                columnFamilies.add(in.readUTF());
            }

            long firstTimestamp = in.readLong();
            int firstIndex = in.readInt();
            return new HBaseSourceSplit(
                    id, host, columnFamilies, Tuple2.of(firstTimestamp, firstIndex));
        }
    }
}
