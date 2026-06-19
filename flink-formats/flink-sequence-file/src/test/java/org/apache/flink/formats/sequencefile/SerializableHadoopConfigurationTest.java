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

package org.apache.flink.formats.sequencefile;

import org.apache.hadoop.conf.Configuration;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/** Tests for the {@link SerializableHadoopConfiguration}. */
class SerializableHadoopConfigurationTest {

    private static final String TEST_KEY = "test-key";

    private static final String TEST_VALUE = "test-value";

    private Configuration configuration;

    @BeforeEach
    void createConfigWithCustomProperty() {
        this.configuration = new Configuration();
        configuration.set(TEST_KEY, TEST_VALUE);
    }

    @Test
    void customPropertiesSurviveSerializationDeserialization()
            throws IOException, ClassNotFoundException {
        final SerializableHadoopConfiguration serializableConfigUnderTest =
                new SerializableHadoopConfiguration(configuration);
        final byte[] serializedConfigUnderTest = serializeAndGetBytes(serializableConfigUnderTest);
        final SerializableHadoopConfiguration deserializableConfigUnderTest =
                deserializeAndGetConfiguration(serializedConfigUnderTest);

        Assertions.<Configuration>assertThat(deserializableConfigUnderTest.get())
                .describedAs(
                        "a Hadoop Configuration with property: key=%s and value=%s",
                        TEST_KEY, TEST_VALUE)
                .satisfies(
                        actualConfig -> {
                            Assertions.assertThat(actualConfig)
                                    .isNotSameAs(serializableConfigUnderTest.get());
                            Assertions.assertThat(actualConfig.get(TEST_KEY))
                                    .isEqualTo(serializableConfigUnderTest.get().get(TEST_KEY));
                        });
    }

    // ----------------------------------------	Helper Methods
    // ---------------------------------------- //

    private byte[] serializeAndGetBytes(SerializableHadoopConfiguration serializableConfigUnderTest)
            throws IOException {
        try (ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(byteStream)) {
            out.writeObject(serializableConfigUnderTest);
            out.flush();
            return byteStream.toByteArray();
        }
    }

    private SerializableHadoopConfiguration deserializeAndGetConfiguration(byte[] serializedConfig)
            throws IOException, ClassNotFoundException {
        try (ObjectInputStream in =
                new ObjectInputStream(new ByteArrayInputStream(serializedConfig))) {
            return (SerializableHadoopConfiguration) in.readObject();
        }
    }
}
