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

package org.apache.flink.connector.rabbitmq2.source.split;

import org.apache.flink.connector.rabbitmq2.RabbitMQConnectionConfig;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Set;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer serializer} for {@link
 * RabbitMQSourceSplit}.
 *
 * @see RabbitMQSourceSplit
 */
public class RabbitMQSourceSplitSerializer
        implements SimpleVersionedSerializer<RabbitMQSourceSplit> {
    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(RabbitMQSourceSplit rabbitMQSourceSplit) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos);
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(out)) {
            objectOutputStream.writeObject(rabbitMQSourceSplit.getConnectionConfig());
            out.writeUTF(rabbitMQSourceSplit.getQueueName());
            writeStringSet(out, rabbitMQSourceSplit.getCorrelationIds());
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public RabbitMQSourceSplit deserialize(int i, byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
                DataInputStream in = new DataInputStream(bais);
                ObjectInputStream objectInputStream = new ObjectInputStream(in)) {
            RabbitMQConnectionConfig config =
                    (RabbitMQConnectionConfig) objectInputStream.readObject();
            String queueName = in.readUTF();
            Set<String> correlationIds = readStringSet(in);
            return new RabbitMQSourceSplit(config, queueName, correlationIds);
        } catch (ClassNotFoundException e) {
            throw new IOException(e.getException());
        }
    }

    private static void writeStringSet(DataOutputStream out, Set<String> strings)
            throws IOException {
        out.writeInt(strings.size());
        for (String string : strings) {
            out.writeUTF(string);
        }
    }

    private static Set<String> readStringSet(DataInputStream in) throws IOException {
        final int len = in.readInt();
        final Set<String> strings = new HashSet<>();
        for (int i = 0; i < len; i++) {
            strings.add(in.readUTF());
        }
        return strings;
    }
}
