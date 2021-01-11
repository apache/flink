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
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

/** Tests for {@link ParameterTool}. */
public class ParameterToolTest extends AbstractParameterToolTest {

    // ----- Parser tests -----------------

    @Test
    public void testFromCliArgs() {
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
        Assertions.assertEquals(7, parameter.getNumberOfParameters());
        validate(parameter);
        Assertions.assertTrue(parameter.has("withoutValues"));
        Assertions.assertEquals(-0.58, parameter.getFloat("negativeFloat"), 0.1);
        Assertions.assertTrue(parameter.getBoolean("isWorking"));
        Assertions.assertEquals(127, parameter.getByte("maxByte"));
        Assertions.assertEquals(-1024, parameter.getShort("negativeShort"));
    }

    @Test
    public void testFromPropertiesFile() throws IOException {
        File propertiesFile = tmp.newFile();
        Properties props = new Properties();
        props.setProperty("input", "myInput");
        props.setProperty("expectedCount", "15");
        try (final OutputStream out = new FileOutputStream(propertiesFile)) {
            props.store(out, "Test properties");
        }
        ParameterTool parameter =
                ParameterTool.fromPropertiesFile(propertiesFile.getAbsolutePath());
        Assertions.assertEquals(2, parameter.getNumberOfParameters());
        validate(parameter);

        parameter = ParameterTool.fromPropertiesFile(propertiesFile);
        Assertions.assertEquals(2, parameter.getNumberOfParameters());
        validate(parameter);

        try (FileInputStream fis = new FileInputStream(propertiesFile)) {
            parameter = ParameterTool.fromPropertiesFile(fis);
        }
        Assertions.assertEquals(2, parameter.getNumberOfParameters());
        validate(parameter);
    }

    @Test
    public void testFromMapOrProperties() {
        Properties props = new Properties();
        props.setProperty("input", "myInput");
        props.setProperty("expectedCount", "15");
        ParameterTool parameter = ParameterTool.fromMap((Map) props);
        Assertions.assertEquals(2, parameter.getNumberOfParameters());
        validate(parameter);
    }

    /** This is mainly meant to be used with -D arguments against the JVM. */
    @Test
    public void testSystemProperties() {
        System.setProperty("input", "myInput");
        System.setProperty("expectedCount", "15");
        ParameterTool parameter = ParameterTool.fromSystemProperties();
        validate(parameter);
    }

    @Test
    public void testMerged() {
        ParameterTool parameter1 =
                (ParameterTool) createParameterToolFromArgs(new String[] {"--input", "myInput"});
        System.setProperty("expectedCount", "15");
        ParameterTool parameter2 = ParameterTool.fromSystemProperties();
        ParameterTool parameter = parameter1.mergeWith(parameter2);
        validate(parameter);
    }

    /** Tests that we can concurrently serialize and access the ParameterTool. See FLINK-7943 */
    @Test
    public void testConcurrentExecutionConfigSerialization()
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
