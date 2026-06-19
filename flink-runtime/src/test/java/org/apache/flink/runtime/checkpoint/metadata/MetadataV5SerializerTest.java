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

import org.apache.flink.runtime.checkpoint.OperatorState;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Random;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

/** {@link MetadataV5Serializer} test. */
class MetadataV5SerializerTest {

    private static final MetadataSerializer INSTANCE = MetadataV5Serializer.INSTANCE;

    private static final Random RND = new Random();

    private Collection<OperatorState> taskStates;

    private CheckpointMetadata metadata;

    @BeforeEach
    public void beforeEach(@TempDir Path tempDir) throws IOException {
        taskStates =
                CheckpointTestUtils.createOperatorStates(
                        RND, tempDir.toUri().toString(), 1, 0, 0, 0);
        metadata = new CheckpointMetadata(1L, taskStates, emptyList(), null);
    }

    @Test
    void testSerializeOperatorUidAndName() throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out)) {
            INSTANCE.serialize(metadata, dos);

            try (DataInputStream dis =
                    new DataInputStream(new ByteArrayInputStream(out.toByteArray()))) {
                CheckpointMetadata deserializedMetadata =
                        INSTANCE.deserialize(dis, metadata.getClass().getClassLoader(), "");
                Collection<OperatorState> operatorStates = deserializedMetadata.getOperatorStates();
                assertThat(operatorStates).hasSize(1);
                OperatorState operatorState = operatorStates.iterator().next();
                assertThat(operatorState.getOperatorName()).isPresent();
                assertThat(operatorState.getOperatorName().get()).isEqualTo("operatorName-0");
                assertThat(operatorState.getOperatorUid()).isPresent();
                assertThat(operatorState.getOperatorUid().get()).isEqualTo("operatorUid-0");
            }
        }
    }

    @Test
    void testSerializeOperatorNameWithEmptyValue() throws IOException {
        taskStates.iterator().next().setOperatorName("");
        testSerializeOperatorWithEmptyValue("Empty string operator name is not allowed");
    }

    @Test
    void testSerializeOperatorUidWithEmptyValue() throws IOException {
        taskStates.iterator().next().setOperatorUid("");
        testSerializeOperatorWithEmptyValue("Empty string operator uid is not allowed");
    }

    void testSerializeOperatorWithEmptyValue(String exceptionMessage) throws IOException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream();
                DataOutputStream dos = new DataOutputStream(out)) {
            final IllegalArgumentException exception =
                    assertThrows(
                            IllegalArgumentException.class,
                            () -> INSTANCE.serialize(metadata, dos));
            assertThat(exception.getMessage()).contains(exceptionMessage);
        }
    }
}
