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

package org.apache.flink.table.gateway.service.application;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.cli.ExecutionConfigAccessor;
import org.apache.flink.client.deployment.application.ApplicationDispatcherLeaderProcessFactoryFactory;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.runner.DefaultDispatcherRunnerFactory;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.DispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;
import org.apache.flink.runtime.rest.ApplicationRestEndpointFactory;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.table.gateway.service.utils.MockHttpServer;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.runtime.application.SqlDriver;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.UserClassLoaderJarTestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

import static org.apache.flink.configuration.ConfigConstants.ENV_FLINK_CONF_DIR;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_LOWER_UDF_CODE;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_UPPER_UDF_CLASS;
import static org.apache.flink.table.utils.UserDefinedFunctions.GENERATED_UPPER_UDF_CODE;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase to verify {@link ScriptRunner}. */
class ScriptRunnerITCase {

    private static Map<String, String> originalEnv;
    private static File udfJar;

    private OutputStream outputStream;

    @BeforeAll
    static void beforeAll(@TempDir File flinkHome, @TempDir Path functionHome) throws Exception {
        originalEnv = System.getenv();
        // prepare yaml
        File confYaml = new File(flinkHome, "config.yaml");
        if (!confYaml.createNewFile()) {
            throw new IOException("Can't create testing config.yaml file.");
        }
        Map<String, String> map = new HashMap<>(System.getenv());
        map.put(ENV_FLINK_CONF_DIR, flinkHome.getAbsolutePath());
        org.apache.flink.core.testutils.CommonTestUtils.setEnv(map);

        Map<String, String> classNameCodes = new HashMap<>();
        classNameCodes.put(
                GENERATED_LOWER_UDF_CLASS,
                String.format(GENERATED_LOWER_UDF_CODE, GENERATED_LOWER_UDF_CLASS));
        classNameCodes.put(
                GENERATED_UPPER_UDF_CLASS,
                String.format(GENERATED_UPPER_UDF_CODE, GENERATED_UPPER_UDF_CLASS));

        udfJar =
                UserClassLoaderJarTestUtils.createJarFile(
                        Files.createTempDirectory(functionHome, "test-jar").toFile(),
                        "test-classloader-udf.jar",
                        classNameCodes);
    }

    @BeforeEach
    void beforeEach() {
        outputStream = new ByteArrayOutputStream(1024);
        SqlDriver.enableTestMode(outputStream);
    }

    @AfterEach
    void afterEach() throws Exception {
        outputStream.close();
        SqlDriver.disableTestMode();
    }

    @AfterAll
    static void afterAll() {
        org.apache.flink.core.testutils.CommonTestUtils.setEnv(originalEnv);
    }

    @Test
    void testRunScriptFromFile(@TempDir Path workDir) throws Exception {
        String script =
                String.format(
                        "CREATE TEMPORARY TABLE sink(\n"
                                + "  a STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values'\n"
                                + ");\n"
                                + "ADD JAR '%s';\n"
                                + "CREATE TEMPORARY FUNCTION lower_func AS '%s';\n"
                                + "CREATE TEMPORARY VIEW v(c) AS VALUES ('A'), ('B'), ('C');\n"
                                + "INSERT INTO sink SELECT lower_func(c) FROM v;",
                        udfJar.getAbsolutePath(), GENERATED_LOWER_UDF_CLASS);

        List<String> arguments =
                Arrays.asList("--scriptUri", createStatementFile(workDir, script).toString());
        runScriptInCluster(arguments);

        assertThat(TestValuesTableFactory.getResultsAsStrings("sink"))
                .containsExactly("+I[a]", "+I[b]", "+I[c]");
    }

    @Test
    void testRunScriptFromRemoteFile(@TempDir Path workDir) throws Exception {
        String script =
                String.format(
                        "CREATE TEMPORARY TABLE sink(\n"
                                + "  a STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values'\n"
                                + ");\n"
                                + "ADD JAR '%s';\n"
                                + "CREATE TEMPORARY FUNCTION lower_func AS '%s';\n"
                                + "CREATE TEMPORARY VIEW v(c) AS VALUES ('A'), ('B'), ('C');\n"
                                + "INSERT INTO sink SELECT lower_func(c) FROM v;",
                        udfJar.getAbsolutePath(), GENERATED_LOWER_UDF_CLASS);
        File file = createStatementFile(workDir, script).toFile();

        try (MockHttpServer server = MockHttpServer.startHttpServer()) {
            URL url = server.prepareResource("/download/script.sql", file);
            List<String> arguments = Arrays.asList("--scriptUri", url.toString());
            runScriptInCluster(arguments);
        }
    }

    @Test
    void testRunScript() throws Exception {
        List<String> arguments =
                Arrays.asList(
                        "--script",
                        String.format(
                                "CREATE TEMPORARY TABLE sink(\n"
                                        + "  a STRING\n"
                                        + ") WITH (\n"
                                        + "  'connector' = 'values'\n"
                                        + ");\n"
                                        + "CREATE TEMPORARY FUNCTION upper_func AS '%s' USING JAR '%s';\n"
                                        + "CREATE TEMPORARY VIEW v(c) AS VALUES ('a'), ('b'), ('c');\n"
                                        + "INSERT INTO sink SELECT upper_func(c) FROM v;",
                                GENERATED_UPPER_UDF_CLASS, udfJar.getAbsolutePath()));
        runScriptInCluster(arguments);

        assertThat(TestValuesTableFactory.getResultsAsStrings("sink"))
                .containsExactly("+I[A]", "+I[B]", "+I[C]");
    }

    void runScriptInCluster(List<String> arguments) throws Exception {
        JobID jobID = JobID.generate();
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
        configuration.set(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH, false);
        configuration.set(DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR, true);
        configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobID.toHexString());

        final TestingMiniClusterConfiguration clusterConfiguration =
                TestingMiniClusterConfiguration.newBuilder()
                        .setConfiguration(configuration)
                        .build();

        PackagedProgram.Builder builder =
                PackagedProgram.newBuilder()
                        .setEntryPointClassName(SqlDriver.class.getName())
                        .setArguments(arguments.toArray(new String[0]))
                        .setUserClassPaths(
                                ExecutionConfigAccessor.fromConfiguration(configuration)
                                        .getClasspaths());
        final TestingMiniCluster.Builder clusterBuilder =
                TestingMiniCluster.newBuilder(clusterConfiguration)
                        .setDispatcherResourceManagerComponentFactorySupplier(
                                createApplicationModeDispatcherResourceManagerComponentFactorySupplier(
                                        clusterConfiguration.getConfiguration(), builder.build()));

        try (final MiniCluster cluster = clusterBuilder.build()) {

            // start mini cluster and submit the job
            cluster.start();

            // wait job finishes.
            awaitJobStatus(cluster, jobID, JobStatus.FINISHED);
        }
    }

    private static Path createStatementFile(Path workDir, String script) throws Exception {
        File file = new File(workDir.toString(), "statement.sql");
        assertThat(file.createNewFile()).isTrue();
        FileUtils.writeFileUtf8(file, script);
        return file.toPath();
    }

    private static Supplier<DispatcherResourceManagerComponentFactory>
            createApplicationModeDispatcherResourceManagerComponentFactorySupplier(
                    Configuration configuration, PackagedProgram program) {
        return () -> {
            final ApplicationDispatcherLeaderProcessFactoryFactory
                    applicationDispatcherLeaderProcessFactoryFactory =
                            ApplicationDispatcherLeaderProcessFactoryFactory.create(
                                    new Configuration(configuration),
                                    SessionDispatcherFactory.INSTANCE,
                                    program);
            return new DefaultDispatcherResourceManagerComponentFactory(
                    new DefaultDispatcherRunnerFactory(
                            applicationDispatcherLeaderProcessFactoryFactory),
                    StandaloneResourceManagerFactory.getInstance(),
                    ApplicationRestEndpointFactory.INSTANCE);
        };
    }

    private static void awaitJobStatus(MiniCluster cluster, JobID jobId, JobStatus status)
            throws Exception {
        CommonTestUtils.waitUntilCondition(
                () -> {
                    try {
                        return cluster.getJobStatus(jobId).get() == status;
                    } catch (ExecutionException e) {
                        if (ExceptionUtils.findThrowable(e, FlinkJobNotFoundException.class)
                                .isPresent()) {
                            // job may not be yet submitted
                            return false;
                        }
                        throw e;
                    }
                },
                500,
                60);
    }
}
