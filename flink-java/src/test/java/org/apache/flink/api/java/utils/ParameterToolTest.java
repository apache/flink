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

package org.apache.flink.api.java.utils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/** Tests for {@link ParameterTool}. */
class ParameterToolTest extends AbstractParameterToolTest {

    // ----- Parser tests -----------------

    @Test
    void testFromCliArgs() {
        ParameterTool parameter =
                (ParameterTool)
                        createParameterToolFromArgs(
                                new String[] {
                                    "--input",
                                    "myInput",
                                    "-expectedCount",
                                    "15",
                                    "--withoutValues",
                                    "--negativeFloat",
                                    "-0.58",
                                    "-isWorking",
                                    "true",
                                    "--maxByte",
                                    "127",
                                    "-negativeShort",
                                    "-1024"
                                });
        assertThat(parameter.getNumberOfParameters()).isEqualTo(7);
        validate(parameter);
        assertThat(parameter.has("withoutValues")).isTrue();
        assertThat(parameter.getFloat("negativeFloat")).isCloseTo(-0.58f, offset(0.1f));
        assertThat(parameter.getBoolean("isWorking")).isTrue();
        assertThat(parameter.getByte("maxByte")).isEqualTo((byte) 127);
        assertThat(parameter.getShort("negativeShort")).isEqualTo((short) -1024);
    }

    @Test
    void testFromPropertiesFile(@TempDir File propertiesFile) throws IOException {
        Properties props = new Properties();
        props.setProperty("input", "myInput");
        props.setProperty("expectedCount", "15");
        Path path = new File(propertiesFile, UUID.randomUUID().toString()).toPath();
        try (final OutputStream out = Files.newOutputStream(path)) {
            props.store(out, "Test properties");
        }
        ParameterTool parameter = ParameterTool.fromPropertiesFile(path.toFile());
        assertThat(parameter.getNumberOfParameters()).isEqualTo(2);
        validate(parameter);

        parameter = ParameterTool.fromPropertiesFile(path.toFile());
        assertThat(parameter.getNumberOfParameters()).isEqualTo(2);
        validate(parameter);

        try (FileInputStream fis = new FileInputStream(path.toFile())) {
            parameter = ParameterTool.fromPropertiesFile(fis);
        }
        assertThat(parameter.getNumberOfParameters()).isEqualTo(2);
        validate(parameter);
    }

    @Test
    void testFromMapOrProperties() {
        Properties props = new Properties();
        props.setProperty("input", "myInput");
        props.setProperty("expectedCount", "15");
        ParameterTool parameter = ParameterTool.fromMap((Map) props);
        assertThat(parameter.getNumberOfParameters()).isEqualTo(2);
        validate(parameter);
    }

    /** This is mainly meant to be used with -D arguments against the JVM. */
    @Test
    void testSystemProperties() {
        System.setProperty("input", "myInput");
        System.setProperty("expectedCount", "15");
        ParameterTool parameter = ParameterTool.fromSystemProperties();
        validate(parameter);
    }

    @Test
    void testMerged() {
        ParameterTool parameter1 =
                (ParameterTool) createParameterToolFromArgs(new String[] {"--input", "myInput"});
        System.setProperty("expectedCount", "15");
        ParameterTool parameter2 = ParameterTool.fromSystemProperties();
        ParameterTool parameter = parameter1.mergeWith(parameter2);
        validate(parameter);
    }

    /** Tests that we can concurrently serialize and access the ParameterTool. See FLINK-7943 */
    @Test
    void testConcurrentExecutionConfigSerialization()
            throws ExecutionException, InterruptedException {

        final int numInputs = 10;
        Collection<String> input = new ArrayList<>(numInputs);

        for (int i = 0; i < numInputs; i++) {
            input.add("--" + UUID.randomUUID());
            input.add(UUID.randomUUID().toString());
        }

        final String[] args = input.toArray(new String[0]);

        final ParameterTool parameterTool = (ParameterTool) createParameterToolFromArgs(args);

        final int numThreads = 5;
        final int numSerializations = 100;

        final Collection<CompletableFuture<Void>> futures = new ArrayList<>(numSerializations);

        final ExecutorService executorService = Executors.newFixedThreadPool(numThreads);

        try {
            for (int i = 0; i < numSerializations; i++) {
                futures.add(
                        CompletableFuture.runAsync(
                                () -> {
                                    try {
                                        serializeDeserialize(parameterTool);
                                    } catch (Exception e) {
                                        throw new CompletionException(e);
                                    }
                                },
                                executorService));
            }

            for (CompletableFuture<Void> future : futures) {
                future.get();
            }
        } finally {
            executorService.shutdownNow();
            executorService.awaitTermination(1000L, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Accesses parameter tool parameters and then serializes the given parameter tool and
     * deserializes again.
     *
     * @param parameterTool to serialize/deserialize
     */
    private void serializeDeserialize(ParameterTool parameterTool)
            throws IOException, ClassNotFoundException {
        // weirdly enough, this call has side effects making the ParameterTool serialization fail if
        // not
        // using a concurrent data structure.
        parameterTool.get(UUID.randomUUID().toString());

        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(parameterTool);
            oos.close();
            baos.close();

            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);

            // this should work :-)
            ParameterTool deserializedParameterTool = ((ParameterTool) ois.readObject());
        }
    }
}
