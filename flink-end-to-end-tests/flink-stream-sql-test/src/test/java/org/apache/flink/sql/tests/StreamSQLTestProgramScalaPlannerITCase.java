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

package org.apache.flink.sql.tests;

import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.FlinkContainersSettings;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.util.Preconditions;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.InspectImageResponse;
import com.github.dockerjava.api.exception.NotFoundException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.DockerClientFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class StreamSQLTestProgramScalaPlannerITCase extends AbstractStreamSQLTestProgramITCase {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(StreamSQLTestProgramScalaPlannerITCase.class);

    @Override
    protected String getPlannerJarName() {
        return "flink-table-planner_";
    }

    @Override
    protected FlinkContainers createFlinkContainers() {
        return FlinkContainers.builder()
                .withFlinkContainersSettings(
                        FlinkContainersSettings.builder().numTaskManagers(4).build())
                .withTestcontainersSettings(
                        TestcontainersSettings.builder().network(NETWORK).build())
                .build();
    }

    @BeforeAll
    static void beforeAll() {
        // Swap planner jar files in `lib/` and `opt/` folders.
        swapPlannerLoaderWithPlannerScala();
        removeBaseImageIfExists();
    }

    @AfterAll
    static void afterAll() {
        // Move back the table-planner-loader as before to `lib/` folder
        swapPlannerScalaWithPlannerLoader();
    }

    private static void removeBaseImageIfExists() {
        final DockerClient client = DockerClientFactory.instance().client();
        InspectImageResponse imageResponse;
        try {
            imageResponse = client.inspectImageCmd("flink-base:latest").exec();
        } catch (final NotFoundException exception) {
            LOGGER.info("Base image does not exist");
            return;
        }
        final String imageId = imageResponse.getId();
        if (imageId != null) {
            client.removeImageCmd(imageId).exec();
            LOGGER.info("Removed base image with id '{}'.", imageResponse.getId());
        }
    }

    private static void swapPlannerLoaderWithPlannerScala() {
        move("flink-table-planner-loader", "lib", "opt");
        move("flink-table-planner_", "opt", "lib");
    }

    private static void swapPlannerScalaWithPlannerLoader() {
        move("flink-table-planner-loader", "opt", "lib");
        move("flink-table-planner_", "lib", "opt");
    }

    private static void move(final String filePattern, final String from, final String to) {
        final String distDir = System.getProperty("distDir");
        Preconditions.checkState(
                distDir != null, "Distribution directory '-DdistDir' parameter is not set.");
        final Path fromDirectory = Paths.get(distDir, from);
        try (final Stream<Path> filesStream = Files.list(fromDirectory)) {
            final List<Path> files =
                    filesStream
                            .filter(entry -> !Files.isDirectory(entry))
                            .filter(file -> file.toString().endsWith(".jar"))
                            .filter(file -> file.getFileName().toString().startsWith(filePattern))
                            .collect(Collectors.toList());
            if (files.size() != 1) {
                throw new IllegalStateException(
                        "Found " + files.size() + " patterns, expected only one.");
            }
            final Path fileToMove = files.get(0);
            Files.move(fileToMove, Paths.get(distDir, to, fileToMove.getFileName().toString()));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
