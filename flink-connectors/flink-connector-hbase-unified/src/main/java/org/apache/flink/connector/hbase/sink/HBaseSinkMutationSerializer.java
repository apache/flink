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

package org.apache.flink.connector.hbase.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.hbase.sink.writer.HBaseWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * This class serializes {@link Mutation} and is used to serialize the state of the {@link
 * HBaseWriter}.
 */
@Internal
class HBaseSinkMutationSerializer implements SimpleVersionedSerializer<Mutation> {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSinkMutationSerializer.class);

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(Mutation mutation) throws IOException {
        LOG.debug("serializing mutation {}", mutation);
        ClientProtos.MutationProto.MutationType type;

        if (mutation instanceof Put) {
            type = ClientProtos.MutationProto.MutationType.PUT;
        } else if (mutation instanceof Delete) {
            type = ClientProtos.MutationProto.MutationType.DELETE;
        } else {
            throw new IllegalArgumentException("Only Put and Delete are supported");
        }

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {
            ProtobufUtil.toMutation(type, mutation).writeDelimitedTo(out);
            return baos.toByteArray();
        }
    }

    @Override
    public Mutation deserialize(int version, byte[] serialized) throws IOException {
        LOG.debug("deserializing mutation");
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {
            ClientProtos.MutationProto proto = ClientProtos.MutationProto.parseDelimitedFrom(in);
            return ProtobufUtil.toMutation(proto);
        }
    }
}
