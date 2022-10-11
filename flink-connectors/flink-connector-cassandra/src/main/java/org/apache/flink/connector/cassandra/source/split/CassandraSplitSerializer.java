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

package org.apache.flink.connector.cassandra.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Serializer for {@link CassandraSplit}. */
public class CassandraSplitSerializer implements SimpleVersionedSerializer<CassandraSplit> {

    public static final CassandraSplitSerializer INSTANCE = new CassandraSplitSerializer();

    public static final int CURRENT_VERSION = 0;

    private CassandraSplitSerializer() {}

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(CassandraSplit cassandraSplit) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream =
                        new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(cassandraSplit);
            objectOutputStream.flush();
            return byteArrayOutputStream.toByteArray();
        }
    }

    @Override
    public CassandraSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serialized);
                ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            return (CassandraSplit) objectInputStream.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }
}
