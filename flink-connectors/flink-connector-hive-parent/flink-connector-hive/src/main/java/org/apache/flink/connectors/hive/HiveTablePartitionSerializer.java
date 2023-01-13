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

package org.apache.flink.connectors.hive;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static org.apache.flink.util.Preconditions.checkArgument;

/** SerDe for {@link HiveTablePartition}. */
public class HiveTablePartitionSerializer implements SimpleVersionedSerializer<HiveTablePartition> {

    private static final int CURRENT_VERSION = 1;

    public static final HiveTablePartitionSerializer INSTANCE = new HiveTablePartitionSerializer();

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(HiveTablePartition hiveTablePartition) throws IOException {
        checkArgument(
                hiveTablePartition.getClass() == HiveTablePartition.class,
                "Cannot serialize subclasses of HiveTablePartition");
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream outputStream = new ObjectOutputStream(byteArrayOutputStream)) {
            outputStream.writeObject(hiveTablePartition);
        }
        return byteArrayOutputStream.toByteArray();
    }

    @Override
    public HiveTablePartition deserialize(int version, byte[] serialized) throws IOException {
        if (version == CURRENT_VERSION) {
            try (ObjectInputStream inputStream =
                    new ObjectInputStream(new ByteArrayInputStream(serialized))) {
                return (HiveTablePartition) inputStream.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException("Failed to deserialize HiveTablePartition", e);
            }
        } else {
            throw new IOException("Unknown version: " + version);
        }
    }
}
