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

package org.apache.flink.fs.gcs.common.writer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GcsRecoverableSerializer implements SimpleVersionedSerializer<GcsRecoverable> {

    static final GcsRecoverableSerializer INSTANCE = new GcsRecoverableSerializer();

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(GcsRecoverable gcsRecoverable) throws IOException {
        Kryo kryo = new Kryo();
        kryo.register(GcsRecoverable.class);
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            try (Output output = new Output(baos)) {
                kryo.writeObject(output, gcsRecoverable);
            }
            return baos.toByteArray();
        }
    }

    @Override
    public GcsRecoverable deserialize(int version, byte[] serialized) throws IOException {
        switch (version) {
            case 1:
                return deserializeV1(serialized);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private static GcsRecoverable deserializeV1(byte[] serialized) {
        Kryo kryo = new Kryo();
        try (Input input = new Input(serialized)) {
            return kryo.readObject(input, GcsRecoverable.class);
        }
    }
}
