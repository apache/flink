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

package org.apache.flink.state.catalog;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.formats.avro.typeutils.AvroTypeInfo;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.catalog.avro.StateTestRecord;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Generates the pre-built savepoint used by {@code
 * StateCatalogGeneratedSavepointITCase.MultiOperatorTypeCatalog}.
 *
 * <p>This file lives in {@code src/test/resources/generator/} so it is NOT compiled as part of the
 * normal build. The generated savepoint lives in {@code src/test/resources/keyed-state-catalog/}
 * and is committed to the repository.
 *
 * <p>To regenerate the savepoint (e.g. after a Flink serializer format change):
 *
 * <ol>
 *   <li>Copy this file to {@code src/test/java/org/apache/flink/state/catalog/}.
 *   <li>Copy {@code src/test/resources/generator/StateTestRecord.avsc} to {@code
 *       src/test/resources/avro/StateTestRecord.avsc} so the Avro class is generated (the {@code
 *       avro-maven-plugin} and {@code flink-avro} dependency are already configured in pom.xml, no
 *       changes needed there).
 *   <li>Remove the {@code @Disabled} annotation and run: {@code ./mvnw test -pl
 *       flink-libraries/flink-state-processing-api
 *       -Dtest=KeyedStateCatalogSavepointGenerator#generateSavepoint}
 *   <li>Verify the new savepoint under {@code src/test/resources/keyed-state-catalog/}.
 *   <li>Remove the copied {@code .java} file from the source tree and the copied {@code .avsc}
 *       file from {@code src/test/resources/avro/}.
 * </ol>
 */
@Disabled("Run manually to regenerate the pre-built savepoint in test resources")
class KeyedStateCatalogSavepointGenerator {

    private static final String RESOURCES_DIR = "src/test/resources/keyed-state-catalog";

    // Must match StateCatalogGeneratedSavepointITCase.MultiOperatorTypeCatalog's UID_* constants.
    private static final String UID_PRIMITIVE = "primitive-state-op";
    private static final String UID_POJO = "pojo-state-op";
    private static final String UID_AVRO_SPECIFIC = "avro-specific-state-op";
    private static final String UID_AVRO_GENERIC = "avro-generic-state-op";

    @Test
    void generateSavepoint() throws Exception {
        Path outputDir = Paths.get(RESOURCES_DIR).toAbsolutePath();
        Files.createDirectories(outputDir);
        deleteExistingSavepoints(outputDir);

        var cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberSlotsPerTaskManager(4)
                                .build());
        cluster.before();
        try {
            Configuration cfg = new Configuration();
            cfg.set(StateBackendOptions.STATE_BACKEND, "hashmap");
            var env = StreamExecutionEnvironment.getExecutionEnvironment(cfg);
            env.setParallelism(2);

            env.addSource(new BoundedWaitingSource(new int[] {1, 2, 3, 4, 5}))
                    .returns(Types.INT)
                    .keyBy(v -> v)
                    .process(new IntCountOperator())
                    .uid(UID_PRIMITIVE)
                    .name(UID_PRIMITIVE)
                    .keyBy(v -> String.valueOf(v))
                    .process(new ProfileOperator())
                    .uid(UID_POJO)
                    .name(UID_POJO)
                    .keyBy(v -> String.valueOf(v))
                    .process(new AvroSpecificOperator())
                    .uid(UID_AVRO_SPECIFIC)
                    .name(UID_AVRO_SPECIFIC)
                    .keyBy(v -> String.valueOf(v))
                    .process(new AvroGenericOperator())
                    .uid(UID_AVRO_GENERIC)
                    .name(UID_AVRO_GENERIC)
                    .sinkTo(new DiscardingSink<>());

            String savepointPath = takeSavepoint(env, outputDir.toString());
            System.out.println("Savepoint written to: " + savepointPath);
        } finally {
            cluster.after();
        }
    }

    private static String takeSavepoint(StreamExecutionEnvironment env, String savepointDir)
            throws Exception {
        JobClient jobClient = env.executeAsync();
        try {
            while (jobClient.getJobStatus().get() != JobStatus.RUNNING) {
                Thread.sleep(100);
            }
            Exception lastEx = null;
            for (int attempt = 0; attempt < 30; attempt++) {
                try {
                    return jobClient
                            .triggerSavepoint(savepointDir, SavepointFormatType.CANONICAL)
                            .get(2, TimeUnit.MINUTES);
                } catch (Exception e) {
                    lastEx = e;
                    Thread.sleep(200);
                }
            }
            throw new RuntimeException("Could not trigger savepoint after 30 attempts", lastEx);
        } finally {
            try {
                jobClient.cancel().get(10, TimeUnit.SECONDS);
            } catch (Exception ignored) {
            }
        }
    }

    /** Deletes any existing {@code savepoint-*} directories inside the given directory. */
    private static void deleteExistingSavepoints(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            return;
        }
        try (var stream = Files.list(dir)) {
            stream.filter(p -> p.getFileName().toString().startsWith("savepoint-"))
                    .filter(Files::isDirectory)
                    .forEach(
                            p -> {
                                try {
                                    deleteDirectory(p);
                                } catch (IOException e) {
                                    throw new RuntimeException("Failed to delete " + p, e);
                                }
                            });
        }
    }

    private static void deleteDirectory(Path dir) throws IOException {
        Files.walkFileTree(
                dir,
                new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                            throws IOException {
                        Files.delete(file);
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path d, IOException exc)
                            throws IOException {
                        Files.delete(d);
                        return FileVisitResult.CONTINUE;
                    }
                });
    }

    // -------------------------------------------------------------------------
    // Source
    // -------------------------------------------------------------------------

    private static class BoundedWaitingSource extends RichSourceFunction<Integer> {

        private final int[] elements;
        private volatile boolean running = true;

        BoundedWaitingSource(int[] elements) {
            this.elements = elements;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            for (int e : elements) {
                ctx.collect(e);
            }
            while (running) {
                Thread.sleep(50);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    // -------------------------------------------------------------------------
    // Operators
    // -------------------------------------------------------------------------

    private static class IntCountOperator extends KeyedProcessFunction<Integer, Integer, Integer> {

        private transient ValueState<Integer> count;

        @Override
        public void open(OpenContext ctx) throws Exception {
            count =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("count", Integer.class));
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<Integer> out)
                throws Exception {
            Integer c = count.value();
            count.update(c == null ? 1 : c + 1);
            out.collect(value);
        }
    }

    private static class ProfileOperator extends KeyedProcessFunction<String, Integer, Integer> {

        private transient ValueState<PersonProfile> profile;

        @Override
        public void open(OpenContext ctx) throws Exception {
            profile =
                    getRuntimeContext()
                            .getState(new ValueStateDescriptor<>("profile", PersonProfile.class));
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<Integer> out)
                throws Exception {
            profile.update(new PersonProfile("name-" + value, value * 10L));
            out.collect(value);
        }
    }

    private static class AvroSpecificOperator
            extends KeyedProcessFunction<String, Integer, Integer> {

        private transient ValueState<StateTestRecord> avroSpecific;

        @Override
        public void open(OpenContext ctx) throws Exception {
            avroSpecific =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "avro_specific",
                                            new AvroTypeInfo<>(StateTestRecord.class)));
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<Integer> out)
                throws Exception {
            var record = new StateTestRecord();
            record.setName("avro-specific-" + value);
            record.setValue((long) value);
            avroSpecific.update(record);
            out.collect(value);
        }
    }

    private static class AvroGenericOperator
            extends KeyedProcessFunction<String, Integer, Integer> {

        private transient ValueState<GenericRecord> avroGeneric;

        @Override
        public void open(OpenContext ctx) throws Exception {
            avroGeneric =
                    getRuntimeContext()
                            .getState(
                                    new ValueStateDescriptor<>(
                                            "avro_generic",
                                            new GenericRecordAvroTypeInfo(
                                                    StateTestRecord.getClassSchema())));
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<Integer> out)
                throws Exception {
            var record = new GenericData.Record(StateTestRecord.getClassSchema());
            record.put("name", "avro-generic-" + value);
            record.put("value", (long) value);
            avroGeneric.update(record);
            out.collect(value);
        }
    }

    // -------------------------------------------------------------------------
    // POJO state type
    // -------------------------------------------------------------------------

    public static class PersonProfile {
        public String name;
        public long score;

        public PersonProfile() {}

        public PersonProfile(String name, long score) {
            this.name = name;
            this.score = score;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PersonProfile)) {
                return false;
            }
            var other = (PersonProfile) o;
            return Objects.equals(name, other.name) && score == other.score;
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, score);
        }
    }
}
