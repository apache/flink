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

package org.apache.flink.connector.testframe.container;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class FlinkImageBuilderTest {
    FlinkContainersSettings settings;
    Configuration conf;

    @TempDir Path tempDir;

    @BeforeEach
    void beforeEach() {
        settings = FlinkContainersSettings.builder().numTaskManagers(2).build();
        conf = settings.getFlinkConfig();
    }

    @Test
    void testFromDistJobManager() throws ImageBuildException {
        ImageFromDockerfile image =
                new FlinkImageBuilder()
                        .setFlinkDistPath(tempDir)
                        .setTempDirectory(tempDir)
                        .setConfiguration(conf)
                        .setLogProperties(settings.getLogProperties())
                        .setBaseImage(settings.getBaseImage())
                        .asJobManager()
                        .build();
        assertThat(image.getDockerImageName()).isEqualTo("flink-configured-jobmanager");
        try (final GenericContainer<?> container = new GenericContainer<>(image)) {
            container
                    .withStartupCheckStrategy(new OneShotStartupCheckStrategy())
                    .withCommand("cat", "/opt/flink/conf/config.yaml")
                    .start();
            assertThat(container.getLogs()).contains("rpc:\n    address: jobmanager");
        }
    }

    @Test
    void testFromDistTaskManager() throws ImageBuildException {
        ImageFromDockerfile image =
                new FlinkImageBuilder()
                        .setFlinkDistPath(tempDir)
                        .setTempDirectory(tempDir)
                        .setConfiguration(conf)
                        .setLogProperties(settings.getLogProperties())
                        .setBaseImage(settings.getBaseImage())
                        .asTaskManager()
                        .build();
        assertThat(image.getDockerImageName()).isEqualTo("flink-configured-taskmanager");
        try (final GenericContainer<?> container = new GenericContainer<>(image)) {
            container
                    .withStartupCheckStrategy(new OneShotStartupCheckStrategy())
                    .withCommand("cat", "/opt/flink/conf/config.yaml")
                    .start();
            assertThat(container.getLogs()).contains("rpc:\n    address: jobmanager");
        }
    }

    @Test
    void testFromDistTaskManagerWithHostParameter() throws ImageBuildException {
        final Configuration taskManagerConf = new Configuration();
        taskManagerConf.addAll(conf);
        taskManagerConf.set(TaskManagerOptions.HOST, "taskmanager1-host");
        ImageFromDockerfile image =
                new FlinkImageBuilder()
                        .setFlinkDistPath(tempDir)
                        .setTempDirectory(tempDir)
                        .setConfiguration(taskManagerConf)
                        .setLogProperties(settings.getLogProperties())
                        .setBaseImage(settings.getBaseImage())
                        .asTaskManager()
                        .build();
        assertThat(image.getDockerImageName()).isEqualTo("flink-configured-taskmanager");
        try (final GenericContainer<?> container = new GenericContainer<>(image)) {
            container
                    .withStartupCheckStrategy(new OneShotStartupCheckStrategy())
                    .withCommand("cat", "/opt/flink/conf/config.yaml")
                    .start();
            assertThat(container.getLogs()).contains("host: taskmanager1-host");
        }
    }

    @Test
    void testFromDistWithLog4jFile() throws ImageBuildException {
        ImageFromDockerfile image =
                new FlinkImageBuilder()
                        .setFlinkDistPath(tempDir)
                        .setTempDirectory(tempDir)
                        .setConfiguration(conf)
                        .setLogProperties(settings.getLogProperties())
                        .setBaseImage(settings.getBaseImage())
                        .asJobManager()
                        .build();
        try (final GenericContainer<?> container = new GenericContainer<>(image)) {
            container
                    .withStartupCheckStrategy(new OneShotStartupCheckStrategy())
                    .withCommand("cat", "/opt/flink/conf/log4j-console.properties")
                    .start();
            System.out.println(container.getLogs());
            assertThat(container.getLogs()).contains("appender.console.name=ConsoleAppender");
        }
    }

    @Test
    void testJobManagerWithBaseImage() throws ImageBuildException {
        final FlinkContainersSettings baseImageSettings =
                FlinkContainersSettings.builder()
                        .numTaskManagers(2)
                        .baseImage("flink:1.19.0-scala_2.12-java11")
                        .build();
        final Configuration baseImageConf = baseImageSettings.getFlinkConfig();

        ImageFromDockerfile image =
                new FlinkImageBuilder()
                        .setTempDirectory(tempDir)
                        .setConfiguration(baseImageConf)
                        .setLogProperties(baseImageSettings.getLogProperties())
                        .setBaseImage(baseImageSettings.getBaseImage())
                        .asJobManager()
                        .build();
        try (final GenericContainer<?> container = new GenericContainer<>(image)) {
            container
                    .withStartupCheckStrategy(new OneShotStartupCheckStrategy())
                    .withCommand("cat", "/opt/flink/conf/config.yaml")
                    .start();
            final String containerShortId = container.getContainerId().substring(0, 12);
            assertThat(container.getLogs()).contains("rpc:\n    address: " + containerShortId);
        }
    }
}
