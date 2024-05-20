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

import org.junit.jupiter.api.AfterAll;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class StreamSQLTestProgramScalaPlannerITCase extends AbstractStreamSQLTestProgramITCase {
    private static final String DIST_DIR = System.getProperty("distDir");

    @Override
    protected FlinkContainers createFlinkContainers() {
        // Swap planner jar files in `lib/` and `opt/` folders
        swapPlannerLoaderWithPlannerScala();

        return FlinkContainers.builder()
                .withFlinkContainersSettings(
                        FlinkContainersSettings.builder().numTaskManagers(4).build())
                .withTestcontainersSettings(
                        TestcontainersSettings.builder().network(NETWORK).build())
                .build();
    }

    @AfterAll
    static void afterAll() {
        swapPlannerScalaWithPlannerLoader();
    }

    private void swapPlannerLoaderWithPlannerScala() {
        move("flink-table-planner-loader", "lib", "opt");
        move("flink-table-planner_", "opt", "lib");
    }

    private static void swapPlannerScalaWithPlannerLoader() {
        move("flink-table-planner-loader", "opt", "lib");
        move("flink-table-planner_", "lib", "opt");
    }

    private static void move(final String filePattern, final String from, final String to) {
        final Path fromDirectory = Paths.get(DIST_DIR, from);
        try (final Stream<Path> filesStream = Files.list(fromDirectory)) {
            final List<Path> files =
                    filesStream
                            .filter(entry -> !Files.isDirectory(entry))
                            .filter(file -> file.toString().endsWith(".jar"))
                            .filter(file -> file.getFileName().toString().startsWith(filePattern))
                            .collect(Collectors.toList());
            if (files.size() != 1) {
                throw new IllegalStateException(
                        "Found multiple file pattern '" + filePattern + "', expected only one.");
            }
            final Path fileToMove = files.get(0);
            Files.move(fileToMove, Paths.get(DIST_DIR, to, fileToMove.getFileName().toString()));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
