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

import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.JobSubmission;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceExtension;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.JarLocation;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

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
@ExtendWith({ParameterizedTestExtension.class, TestLoggerExtension.class})
class ScalaFreeITCase {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> TEST_EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(
                    java.util.concurrent.Executors::newSingleThreadScheduledExecutor);

    @RegisterExtension private final FlinkResourceExtension flinkExtension;
    private final String mainClass;

    @Parameters(name = "{0}")
    private static Collection<TestParams> testParameters() {
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
                                        ResourceTestUtils.getResource("/scala.jar"),
                                        JarLocation.LIB)));
    }

    private ScalaFreeITCase(TestParams testParams) {
        final FlinkResourceSetup.FlinkResourceSetupBuilder builder =
                FlinkResourceSetup.builder()
                        .moveJar("flink-scala", JarLocation.LIB, JarLocation.OPT);
        testParams.builderSetup.accept(builder);
        flinkExtension = new FlinkResourceExtension(FlinkResource.get(builder.build()));
        mainClass = testParams.mainClass;
    }

    @TestTemplate
    void testScalaFreeJobExecution() throws Exception {
        final Path jobJar = ResourceTestUtils.getResource("/jobs.jar");

        try (final ClusterController clusterController =
                flinkExtension.getFlinkResource().startCluster(1)) {
            clusterController.submitJob(
                    new JobSubmission.JobSubmissionBuilder(jobJar)
                            .setDetached(false)
                            .setMainClass(mainClass)
                            .build(),
                    Duration.ofHours(1));
        }
    }

    private static class TestParams {

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

        @Override
        public String toString() {
            return description;
        }
    }
}
