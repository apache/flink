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
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.catalog.avro.StateTestRecord;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.legacy.RichSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;

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
 * StateCatalogGeneratedSavepointITCase.OffClasspathKeyTypes} for POJO-key and Avro-specific-key
 * scenarios.
 *
 * <p>This file lives in {@code src/test/java/} only during savepoint generation. After generation,
 * move it to {@code src/test/resources/generator/} so it is NOT compiled as part of the normal
 * build. The generated savepoint is committed to {@code
 * src/test/resources/keyed-state-pojo-avro-key/}.
 *
 * <p>To regenerate the savepoint (e.g. after a Flink serializer format change):
 *
 * <ol>
 *   <li>Copy this file to {@code src/test/java/org/apache/flink/state/catalog/} if not already
 *       there.
 *   <li>Copy {@code src/test/resources/generator/StateTestRecord.avsc} to {@code
 *       src/test/resources/avro/StateTestRecord.avsc} so the Avro class is generated.
 *   <li>Remove the {@code @Disabled} annotation and run: {@code ./mvnw test -pl
 *       flink-libraries/flink-state-processing-api
 *       -Dtest=KeyedStatePojoAvroKeySavepointGenerator#generateSavepoint}
 *   <li>Verify the savepoint under {@code src/test/resources/keyed-state-pojo-avro-key/}.
 *   <li>Remove the copied {@code StateTestRecord.avsc} from {@code src/test/resources/avro/} and
 *       move this {@code .java} file back to {@code resources/generator/}.
 * </ol>
 */
@Disabled("Run manually to regenerate the pre-built savepoint in test resources")
class KeyedStatePojoAvroKeySavepointGenerator {

    /** Operator UIDs referenced by {@code StateCatalogGeneratedSavepointITCase.OffClasspathKeyTypes}. */
    static final String UID_POJO_KEY = "pojo-key-state-op";

    static final String UID_AVRO_SPECIFIC_KEY = "avro-specific-key-state-op";

    private static final String RESOURCES_DIR = "src/test/resources/keyed-state-pojo-avro-key";

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

            // Pipeline 1: POJO key (PersonKey{id,name}).
            // PersonKey is defined only in this file → NOT on the classpath during normal test
            // runs, which exercises the off-classpath POJO-key reading path.
            env.addSource(new BoundedWaitingSource(new int[] {1, 2, 3, 4, 5}))
                    .returns(Types.INT)
                    .keyBy(v -> new PersonKey(v, "name-" + v))
                    .process(new PojoKeyCountOperator())
                    .uid(UID_POJO_KEY)
                    .name(UID_POJO_KEY)
                    .sinkTo(new DiscardingSink<>());

            // Pipeline 2: Avro-specific key (StateTestRecord).
            // StateTestRecord is generated from StateTestRecord.avsc only when the .avsc file is
            // present in src/test/resources/avro/. After generation that file is removed, so the
            // class stays off the classpath during tests, exercising the Avro fallback path.
            env.addSource(new BoundedWaitingSource(new int[] {1, 2, 3, 4, 5}))
                    .returns(Types.INT)
                    .keyBy(
                            (KeySelector<Integer, StateTestRecord>)
                                    v -> {
                                        StateTestRecord r = new StateTestRecord();
                                        r.setName("key-" + v);
                                        r.setValue((long) v);
                                        return r;
                                    })
                    .process(new AvroSpecificKeyCountOperator())
                    .uid(UID_AVRO_SPECIFIC_KEY)
                    .name(UID_AVRO_SPECIFIC_KEY)
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

    /** Counts elements per PersonKey key. Simple Integer value state avoids Avro complications. */
    private static class PojoKeyCountOperator
            extends KeyedProcessFunction<PersonKey, Integer, Integer> {

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

    /** Counts elements per StateTestRecord (Avro-specific) key. */
    private static class AvroSpecificKeyCountOperator
            extends KeyedProcessFunction<StateTestRecord, Integer, Integer> {

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

    // -------------------------------------------------------------------------
    // POJO key type — defined here so it is NOT on the classpath during normal
    // test runs (this file must be excluded from the normal build).
    // -------------------------------------------------------------------------

    /** Key POJO for the {@value #UID_POJO_KEY} operator. */
    public static class PersonKey {
        public int id;
        public String name;

        public PersonKey() {}

        public PersonKey(int id, String name) {
            this.id = id;
            this.name = name;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof PersonKey)) {
                return false;
            }
            PersonKey other = (PersonKey) o;
            return id == other.id && Objects.equals(name, other.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, name);
        }

        @Override
        public String toString() {
            return "PersonKey{id=" + id + ", name='" + name + "'}";
        }
    }
}
