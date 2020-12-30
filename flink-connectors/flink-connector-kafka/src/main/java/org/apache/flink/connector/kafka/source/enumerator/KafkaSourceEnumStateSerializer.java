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

package org.apache.flink.connector.kafka.source.enumerator;

import org.apache.flink.connector.base.source.utils.SerdeUtils;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplit;
import org.apache.flink.connector.kafka.source.split.KafkaPartitionSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The {@link org.apache.flink.core.io.SimpleVersionedSerializer Serializer} for the enumerator
 * state of Kafka source.
 */
public class KafkaSourceEnumStateSerializer
        implements SimpleVersionedSerializer<KafkaSourceEnumState> {

    private static final int CURRENT_VERSION = 0;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(KafkaSourceEnumState enumState) throws IOException {
        return SerdeUtils.serializeSplitAssignments(
                enumState.getCurrentAssignment(), new KafkaPartitionSplitSerializer());
    }

    @Override
    public KafkaSourceEnumState deserialize(int version, byte[] serialized) throws IOException {
        if (version == 0) {
            Map<Integer, Set<KafkaPartitionSplit>> currentPartitionAssignment =
                    SerdeUtils.deserializeSplitAssignments(
                            serialized, new KafkaPartitionSplitSerializer(), HashSet::new);
            return new KafkaSourceEnumState(currentPartitionAssignment);
        }
        throw new IOException(
                String.format(
                        "The bytes are serialized with version %d, "
                                + "while this deserializer only supports version up to %d",
                        version, CURRENT_VERSION));
    }
}
