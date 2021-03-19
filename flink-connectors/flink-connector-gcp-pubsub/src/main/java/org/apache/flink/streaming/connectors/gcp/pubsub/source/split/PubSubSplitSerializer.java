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

package org.apache.flink.streaming.connectors.gcp.pubsub.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * A stub to serialize instances of {@link PubSubSplit}. No real deserialization or serialization is
 * carried out because of how generic the {@link PubSubSplit} is.
 */
public class PubSubSplitSerializer implements SimpleVersionedSerializer<PubSubSplit> {
    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PubSubSplit obj) throws IOException {
        return new byte[0];
    }

    @Override
    public PubSubSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version == 0) {
            return new PubSubSplit();
        }
        throw new IOException(
                String.format(
                        "The bytes are serialized with version %d, "
                                + "while this deserializer only supports version up to %d",
                        version, CURRENT_VERSION));
    }
}
