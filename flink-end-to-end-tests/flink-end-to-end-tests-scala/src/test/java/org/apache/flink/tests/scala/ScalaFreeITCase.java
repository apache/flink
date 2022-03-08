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

package org.apache.flink.tests.scala;

import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.JarLocation;
import org.apache.flink.tests.util.flink.JobSubmission;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

/**
 * Tests that Flink does not require Scala for jobs that do not use the Scala APIs. This covers both
 * pure Java jobs, and Scala jobs that use the Java APIs exclusively with Scala types.
 */
@RunWith(Parameterized.class)
public class ScalaFreeITCase extends TestLogger {

    @Rule
    public final TestExecutorResource<ScheduledExecutorService> testExecutorResource =
            new TestExecutorResource<>(
                    java.util.concurrent.Executors::newSingleThreadScheduledExecutor);

    @Rule public final FlinkResource flink;
    private final String mainClass;

    @Parameterized.Parameters(name = "{index}: {0}")
    public static Collection<TestParams> testParameters() {
        return Arrays.asList(
                new TestParams("Java job, without Scala in lib/", JavaJob.class.getCanonicalName()),
                new TestParams(
                        "Java job with Kryo serializer, without Scala in lib/",
                        JavaJobWithKryoSerializer.class.getCanonicalName()),
                new TestParams(
                        "Scala job, with user-provided Scala in lib/",
                        ScalaJob.class.getCanonicalName(),
                        builder ->
                                builder.addJar(
                                        TestUtils.getResource("/scala.jar"), JarLocation.LIB)));
    }

    public ScalaFreeITCase(TestParams testParams) {
        final FlinkResourceSetup.FlinkResourceSetupBuilder builder =
                FlinkResourceSetup.builder()
                        .moveJar("flink-scala", JarLocation.LIB, JarLocation.OPT);
        testParams.builderSetup.accept(builder);
        flink = FlinkResource.get(builder.build());
        mainClass = testParams.mainClass;
    }

    @Test
    public void testScalaFreeJobExecution() throws Exception {
        final Path jobJar = TestUtils.getResource("/jobs.jar");

        try (final ClusterController clusterController = flink.startCluster(1)) {
            // if the job fails then this throws an exception
            clusterController.submitJob(
                    new JobSubmission.JobSubmissionBuilder(jobJar)
                            .setDetached(false)
                            .setMainClass(mainClass)
                            .build(),
                    Duration.ofHours(1));
        }
    }

    static class TestParams {

        private final String description;
        private final String mainClass;
        private final Consumer<FlinkResourceSetup.FlinkResourceSetupBuilder> builderSetup;

        TestParams(String description, String mainClass) {
            this(description, mainClass, ignored -> {});
        }

        TestParams(
                String description,
                String mainClass,
                Consumer<FlinkResourceSetup.FlinkResourceSetupBuilder> builderSetup) {
            this.description = description;
            this.mainClass = mainClass;
            this.builderSetup = builderSetup;
        }

        public String getMainClass() {
            return mainClass;
        }

        public Consumer<FlinkResourceSetup.FlinkResourceSetupBuilder> getBuilderSetup() {
            return builderSetup;
        }

        @Override
        public String toString() {
            return description;
        }
    }
}
