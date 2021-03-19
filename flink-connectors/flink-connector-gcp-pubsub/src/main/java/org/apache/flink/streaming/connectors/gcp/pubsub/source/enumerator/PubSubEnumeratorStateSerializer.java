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

package org.apache.flink.streaming.connectors.gcp.pubsub.source.enumerator;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;

/**
 * A stub to serialize the contents of a {@link PubSubEnumeratorState}. Because no data is stored in
 * such a checkpoint, no proper serialization is necessary.
 */
public class PubSubEnumeratorStateSerializer
        implements SimpleVersionedSerializer<PubSubEnumeratorState> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(PubSubEnumeratorState enumeratorCheckpoint) throws IOException {
        return new byte[0];
    }

    @Override
    public PubSubEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        return new PubSubEnumeratorState();
    }
}
