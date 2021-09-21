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

package org.apache.flink.tests.util.flink;

import org.apache.flink.client.deployment.StandaloneClusterId;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.apache.flink.tests.util.parameters.ParameterProperty;
import org.apache.flink.tests.util.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.exception.NotFoundException;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.lifecycle.TestDescription;
import org.testcontainers.lifecycle.TestLifecycleAware;
import org.testcontainers.utility.MountableFile;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A container that wraps a Flink distribution and spawns a Flink cluster in a single Docker
 * container.
 */
public class FlinkContainer extends GenericContainer<FlinkContainer> implements TestLifecycleAware {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkContainer.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final String FLINK_BIN = "flink/bin";
    public static final int JOB_MANAGER_REST_PORT = 8081;

    private final TemporaryFolder temporaryFolder = new TemporaryFolder();
    private final Path logBackupDir;
    private final int numTaskManagers;

    @Nullable private RestClusterClient<StandaloneClusterId> restClusterClient;

    private FlinkContainer(
            ImageFromDockerfile image, int numTaskManagers, @Nullable Path logBackupDir) {
        super(image);
        this.logBackupDir = logBackupDir;
        this.numTaskManagers = numTaskManagers;
        withExposedPorts(JOB_MANAGER_REST_PORT);
        // Create a network for connecting with other containers
        withNetwork(Network.newNetwork());
        waitingFor(
                new HttpWaitStrategy()
                        .forPort(JOB_MANAGER_REST_PORT)
                        .forPath("/taskmanagers")
                        .forResponsePredicate(
                                response -> {
                                    try {
                                        TaskManagersInfo managersInfo =
                                                objectMapper.readValue(
                                                        response, TaskManagersInfo.class);
                                        return numTaskManagers
                                                == managersInfo.getTaskManagerInfos().size();
                                    } catch (JsonProcessingException e) {
                                        return false;
                                    }
                                }));
    }

    @Override
    public void beforeTest(TestDescription description) {
        try {
            temporaryFolder.create();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void afterTest(TestDescription description, Optional<Throwable> throwable) {
        if (throwable.isPresent() && logBackupDir != null) {
            try {
                final Path targetDirectory = logBackupDir.resolve("flink-" + UUID.randomUUID());
                copyFileOrDirectoryFromContainer("flink/log/", targetDirectory);
                LOG.info("Backed up logs to {}.", targetDirectory);
            } catch (IOException e) {
                LOG.error("Could not backup the flink logs.", e);
            }
        }
        temporaryFolder.delete();
    }

    @Override
    public void stop() {
        if (restClusterClient != null) {
            restClusterClient.close();
        }
        super.stop();
    }

    /**
     * Get {@link RestClusterClient} connected to this FlinkContainer.
     *
     * <p>This method lazily initializes the REST client on-demand.
     */
    public RestClusterClient<StandaloneClusterId> getRestClusterClient() {
        if (restClusterClient != null) {
            return restClusterClient;
        }
        checkState(
                this.isRunning(), "Cluster client should only be retrieved for a running cluster");
        try {
            final Configuration clientConfiguration = new Configuration();
            clientConfiguration.set(RestOptions.ADDRESS, getHost());
            clientConfiguration.set(RestOptions.PORT, getMappedPort(JOB_MANAGER_REST_PORT));
            this.restClusterClient =
                    new RestClusterClient<>(clientConfiguration, StandaloneClusterId.getInstance());
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Failed to create client for FlinkContainer cluster", e);
        }
        return restClusterClient;
    }

    /**
     * Copies a container path to a local directory. In contrast to {@link
     * GenericContainer#copyFileFromContainer(String, String)} it supports copying whole
     * directories. <b>NOTE:</b> The {@code hostDirectory} should point to a destination directory.
     */
    public void copyFileOrDirectoryFromContainer(String containerPath, Path hostDirectory)
            throws IOException {
        DockerClient dockerClient = DockerClientFactory.instance().client();
        try (InputStream inputStream =
                        dockerClient
                                .copyArchiveFromContainerCmd(getContainerId(), containerPath)
                                .exec();
                TarArchiveInputStream tarInputStream = new TarArchiveInputStream(inputStream)) {
            unTar(tarInputStream, hostDirectory.toFile());
        }
    }

    private static void unTar(TarArchiveInputStream tis, File destFolder) throws IOException {
        TarArchiveEntry entry;
        while ((entry = tis.getNextTarEntry()) != null) {
            FileOutputStream fos = null;
            try {
                if (entry.isDirectory()) {
                    continue;
                }
                File curfile = new File(destFolder, entry.getName());
                File parent = curfile.getParentFile();
                if (!parent.exists()) {
                    parent.mkdirs();
                }
                fos = new FileOutputStream(curfile);
                IOUtils.copyBytes(tis, fos, false);
            } catch (Exception e) {
                LOG.warn("Exception extracting {} to {}", tis, destFolder, e);
            } finally {
                try {
                    if (fos != null) {
                        fos.flush();
                        fos.getFD().sync();
                        fos.close();
                    }
                } catch (IOException e) {
                    LOG.warn("Exception closing {}", fos, e);
                }
            }
        }
    }

    /**
     * Submits a SQL job to the running cluster.
     *
     * <p><b>NOTE:</b> You should not use {@code '\t'}.
     */
    public void submitSQLJob(SQLJobSubmission job) throws IOException, InterruptedException {
        final List<String> commands = new ArrayList<>();
        Path script = temporaryFolder.newFile().toPath();
        Files.write(script, job.getSqlLines());
        copyFileToContainer(MountableFile.forHostPath(script), "/tmp/script.sql");
        commands.add("cat /tmp/script.sql | ");
        commands.add(FLINK_BIN + "/sql-client.sh");
        for (String jar : job.getJars()) {
            commands.add("--jar");
            String containerPath = copyAndGetContainerPath(jar);
            commands.add(containerPath);
        }

        ExecResult execResult = execInContainer("bash", "-c", String.join(" ", commands));
        LOG.info(execResult.getStdout());
        LOG.error(execResult.getStderr());
        if (execResult.getExitCode() != 0) {
            throw new AssertionError("Failed when submitting the SQL job.");
        }
    }

    /**
     * Restart all task managers.
     *
     * @param afterFailAction Action to do between stopping and restarting task managers
     * @throws Exception If anything wrong happens during the restart
     */
    public void restartTaskManager(RunnableWithException afterFailAction) throws Exception {
        final String[] stopCommand = {FLINK_BIN + "/taskmanager.sh", "stop-all"};
        final ExecResult stopResult = execInContainer(stopCommand);
        checkExitCode(stopResult, stopCommand);

        afterFailAction.run();

        for (int i = 0; i < numTaskManagers; i++) {
            final String[] startCommand = {FLINK_BIN + "/taskmanager.sh", "start"};
            final ExecResult startResult = execInContainer(startCommand);
            checkExitCode(startResult, startCommand);
        }
    }

    public void checkExitCode(ExecResult execResult, String... command) {
        if (execResult.getExitCode() != 0) {
            throw new IllegalStateException(
                    String.format(
                            "Command \"%s\" exited with code %d. \nSTDOUT: %s\nSTDERR: %s",
                            String.join(" ", command),
                            execResult.getExitCode(),
                            execResult.getStdout(),
                            execResult.getStderr()));
        }
    }

    @Nonnull
    private String copyAndGetContainerPath(String defaultEnvFile) {
        Path path = Paths.get(defaultEnvFile);
        String containerPath = "/tmp/" + path.getFileName();
        copyFileToContainer(MountableFile.forHostPath(path), containerPath);
        return containerPath;
    }

    public static FlinkContainerBuilder builder() {
        return new FlinkContainerBuilder();
    }

    /** A builder for a {@link FlinkContainer}. */
    public static class FlinkContainerBuilder {

        private static final ParameterProperty<Path> DISTRIBUTION_LOG_BACKUP_DIRECTORY =
                new ParameterProperty<>("logBackupDir", Paths::get);

        private int numTaskManagers = 1;
        private Configuration flinkConfiguration;
        private String javaVersion;
        private final TemporaryFolder temporaryFolder = new TemporaryFolder();

        /**
         * The expected number of TaskManagers to start. All TaskManagers are created in the same
         * container along with the JobManager
         */
        public FlinkContainerBuilder numTaskManagers(int numTaskManagers) {
            this.numTaskManagers = numTaskManagers;
            return this;
        }

        /**
         * Additional Flink configurations for the cluster.
         *
         * @param flinkConfiguration Additional Flink configurations
         * @return Builder itself
         */
        public FlinkContainerBuilder withFlinkConfiguration(Configuration flinkConfiguration) {
            this.flinkConfiguration = flinkConfiguration;
            return this;
        }

        /**
         * Specifies which OpenJDK version to use. If not provided explicitly, the image version
         * will be derived based on the version of the java that runs the test.
         */
        public FlinkContainerBuilder javaVersion(String javaVersion) {
            this.javaVersion = javaVersion;
            return this;
        }

        public FlinkContainer build() {
            try {
                Path flinkDist = FileUtils.findFlinkDist();
                Path flinkConfDirectory = flinkDist.resolve("conf");

                // Load Flink configurations
                final Configuration mergedFlinkConfiguration =
                        GlobalConfiguration.loadConfiguration(
                                flinkConfDirectory.toAbsolutePath().toString());

                // Merge additional Flink configurations
                if (flinkConfiguration != null) {
                    mergedFlinkConfiguration.addAll(flinkConfiguration);
                }

                // Create temporary folder for holding configuration files
                temporaryFolder.create();
                Path tmp = temporaryFolder.newFolder().toPath();

                // Write worker file
                Path workersFile = tmp.resolve("workers");
                Files.write(
                        workersFile,
                        IntStream.range(0, numTaskManagers)
                                .mapToObj(i -> "localhost")
                                .collect(Collectors.toList()));

                // Write merged Flink configuration
                Path configurationFile = tmp.resolve(GlobalConfiguration.FLINK_CONF_FILENAME);
                final List<String> configurationLines =
                        mergedFlinkConfiguration.toMap().entrySet().stream()
                                .map(entry -> entry.getKey() + ": " + entry.getValue())
                                .collect(Collectors.toList());
                Files.write(configurationFile, configurationLines);

                // Building the docker image is split into two stages:
                // 1. build a base image with an immutable flink-dist
                // 2. based on the base image add any mutable files such as e.g. workers files
                //
                // This lets us save some time for archiving and copying big, immutable files
                // between tests runs.
                String baseImage = buildBaseImage(flinkDist);
                ImageFromDockerfile configuredImage =
                        buildConfiguredImage(workersFile, configurationFile, baseImage);

                Optional<Path> logBackupDirectory = DISTRIBUTION_LOG_BACKUP_DIRECTORY.get();
                if (!logBackupDirectory.isPresent()) {
                    LOG.warn(
                            "Property {} not set, logs will not be backed up in case of test failures.",
                            DISTRIBUTION_LOG_BACKUP_DIRECTORY.getPropertyName());
                }
                return new FlinkContainer(
                        configuredImage, numTaskManagers, logBackupDirectory.orElse(null));
            } catch (Exception e) {
                temporaryFolder.delete();
                throw new RuntimeException("Could not build the flink-dist image", e);
            }
        }

        private ImageFromDockerfile buildConfiguredImage(
                Path workersFile, Path configurationFile, String baseImage) {
            return new ImageFromDockerfile("flink-dist-configured")
                    .withDockerfileFromBuilder(
                            builder ->
                                    builder.from(baseImage)
                                            .copy("workers", "flink/conf/workers")
                                            .copy(
                                                    GlobalConfiguration.FLINK_CONF_FILENAME,
                                                    "flink/conf/"
                                                            + GlobalConfiguration
                                                                    .FLINK_CONF_FILENAME)
                                            .cmd(
                                                    FLINK_BIN
                                                            + "/start-cluster.sh && tail -f /dev/null")
                                            .build())
                    .withFileFromPath("workers", workersFile)
                    .withFileFromPath(GlobalConfiguration.FLINK_CONF_FILENAME, configurationFile);
        }

        @Nonnull
        private String buildBaseImage(Path flinkDist) throws java.util.concurrent.TimeoutException {
            String baseImage = "flink-dist-base";
            if (!imageExists(baseImage)) {
                new ImageFromDockerfile(baseImage)
                        .withDockerfileFromBuilder(
                                builder ->
                                        builder.from("openjdk:" + getJavaVersionSuffix())
                                                .copy("flink", "flink")
                                                .build())
                        .withFileFromPath("flink", flinkDist)
                        .get(4, TimeUnit.MINUTES);
            }
            return baseImage;
        }

        private boolean imageExists(String baseImage) {
            try {
                DockerClientFactory.instance().client().inspectImageCmd(baseImage).exec();
                return true;
            } catch (NotFoundException e) {
                return false;
            }
        }

        private String getJavaVersionSuffix() {
            if (javaVersion != null) {
                return javaVersion;
            } else {
                String javaSpecVersion = System.getProperty("java.vm.specification.version");
                switch (javaSpecVersion) {
                    case "1.8":
                        return "8";
                    case "11":
                        return "11";
                    default:
                        throw new IllegalStateException("Unexpected value: " + javaSpecVersion);
                }
            }
        }
    }
}
