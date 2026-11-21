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

package org.apache.flink.runtime.entrypoint.component;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever.JOB_GRAPH_FILE_PATH;
import static org.apache.flink.testutils.junit.utils.TempDirUtils.newFile;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link FileJobGraphRetriever}. */
class FileJobGraphRetrieverTest {

    @TempDir private static Path classTemporaryFolder;

    private static Configuration configuration;

    private static Path jarFileInJobGraph;

    @BeforeAll
    static void init() throws IOException {
        final Path jobGraphPath =
                newFile(classTemporaryFolder, JOB_GRAPH_FILE_PATH.defaultValue()).toPath();

        jarFileInJobGraph = newFile(classTemporaryFolder, "jar_file_in_job_graph.jar").toPath();

        final JobVertex source = new JobVertex("source");
        final JobVertex target = new JobVertex("target");
        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertices(Arrays.asList(source, target))
                        .addClasspaths(Collections.singletonList(jarFileInJobGraph.toUri().toURL()))
                        .build();

        try (ObjectOutputStream objectOutputStream =
                new ObjectOutputStream(Files.newOutputStream(jobGraphPath, CREATE))) {
            objectOutputStream.writeObject(jobGraph);
        }
        final Configuration cfg = new Configuration();
        cfg.setString(JOB_GRAPH_FILE_PATH.key(), jobGraphPath.toString());
        configuration = new UnmodifiableConfiguration(cfg);
    }

    @Test
    void testRetrieveJobGraphWithJarInUsrLibDir(@TempDir File usrLibDir)
            throws IOException, FlinkException {
        final File jarFileInUsrLibDir =
                Files.createFile(usrLibDir.toPath().resolve("jar_file_in_job_dir.jar")).toFile();
        final Path workingDirectory = FileUtils.getCurrentWorkingDirectory();
        final Path relativeJarFileInUsrLibDirPath =
                FileUtils.relativizePath(workingDirectory, jarFileInUsrLibDir.toPath());

        final FileJobGraphRetriever fileJobGraphRetriever =
                FileJobGraphRetriever.createFrom(configuration, usrLibDir);
        final JobGraph jobGraphFromFile = fileJobGraphRetriever.retrieveJobGraph(configuration);

        assertThat(jobGraphFromFile.getClasspaths())
                .containsExactlyInAnyOrder(
                        jarFileInJobGraph.toUri().toURL(),
                        FileUtils.toURL(relativeJarFileInUsrLibDirPath));
    }

    @Test
    void testRetrieveJobGraphWithoutUsrLibDir() throws IOException, FlinkException {
        final FileJobGraphRetriever fileJobGraphRetriever =
                FileJobGraphRetriever.createFrom(configuration, null);
        final JobGraph jobGraph = fileJobGraphRetriever.retrieveJobGraph(configuration);

        assertThat(jobGraph.getClasspaths()).containsExactly(jarFileInJobGraph.toUri().toURL());
    }
}
