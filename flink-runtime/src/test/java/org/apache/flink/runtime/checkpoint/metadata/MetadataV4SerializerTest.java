package org.apache.flink.runtime.checkpoint.metadata;

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

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointProperties;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link MetadataV4Serializer} test. */
class MetadataV4SerializerTest {

    @Test
    void testSerializeProperties() throws IOException {
        CheckpointMetadata metadata =
                new CheckpointMetadata(
                        1L,
                        emptyList(),
                        emptyList(),
                        CheckpointProperties.forSavepoint(false, SavepointFormatType.NATIVE));

        MetadataSerializer instance = MetadataV4Serializer.INSTANCE;

        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out)) {
            instance.serialize(metadata, dos);

            try (DataInputStream dis =
                    new DataInputStream(new ByteArrayInputStream(out.toByteArray()))) {
                assertThat(
                                instance.deserialize(dis, metadata.getClass().getClassLoader(), "")
                                        .getCheckpointProperties())
                        .isEqualTo(metadata.getCheckpointProperties());
            }
        }
    }
}
