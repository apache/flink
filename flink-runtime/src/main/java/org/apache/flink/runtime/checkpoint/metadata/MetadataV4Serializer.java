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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * V4 serializer that adds {@link org.apache.flink.runtime.checkpoint.CheckpointProperties}
 * serialization.
 */
@Internal
public class MetadataV4Serializer implements MetadataSerializer {

    public static final MetadataSerializer INSTANCE = new MetadataV4Serializer();
    public static final int VERSION = 4;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public CheckpointMetadata deserialize(
            DataInputStream dis, ClassLoader userCodeClassLoader, String externalPointer)
            throws IOException {
        return MetadataV3Serializer.INSTANCE
                .deserialize(dis, userCodeClassLoader, externalPointer)
                .withProperties(deserializeProperties(dis));
    }

    @Override
    public void serialize(CheckpointMetadata checkpointMetadata, DataOutputStream dos)
            throws IOException {
        MetadataV3Serializer.INSTANCE.serialize(checkpointMetadata, dos);
        serializeProperties(checkpointMetadata.getCheckpointProperties(), dos);
    }

    private CheckpointProperties deserializeProperties(DataInputStream dis) throws IOException {
        try {
            // closed outside
            return (CheckpointProperties) new ObjectInputStream(dis).readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Couldn't deserialize checkpoint properties", e);
        }
    }

    private static void serializeProperties(CheckpointProperties properties, DataOutputStream dos)
            throws IOException {
        new ObjectOutputStream(dos).writeObject(properties); // closed outside
    }
}
