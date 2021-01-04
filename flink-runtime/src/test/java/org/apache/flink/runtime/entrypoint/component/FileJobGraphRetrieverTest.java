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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.UnmodifiableConfiguration;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever.JOB_GRAPH_FILE_PATH;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

/** Tests for the {@link FileJobGraphRetriever}. */
public class FileJobGraphRetrieverTest {

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static Configuration configuration;

    private static Path jarFileInJobGraph;

    @BeforeClass
    public static void init() throws IOException {
        final Path jobGraphPath =
                TEMPORARY_FOLDER.newFile(JOB_GRAPH_FILE_PATH.defaultValue()).toPath();

        jarFileInJobGraph = TEMPORARY_FOLDER.newFile("jar_file_in_job_graph.jar").toPath();

        final JobVertex source = new JobVertex("source");
        final JobVertex target = new JobVertex("target");
        final JobGraph jobGraph = new JobGraph(new JobID(), "test", source, target);

        jobGraph.setClasspaths(Collections.singletonList(jarFileInJobGraph.toUri().toURL()));

        try (ObjectOutputStream objectOutputStream =
                new ObjectOutputStream(Files.newOutputStream(jobGraphPath, CREATE))) {
            objectOutputStream.writeObject(jobGraph);
        }
        final Configuration cfg = new Configuration();
        cfg.setString(JOB_GRAPH_FILE_PATH.key(), jobGraphPath.toString());
        configuration = new UnmodifiableConfiguration(cfg);
    }

    @Test
    public void testRetrieveJobGraphWithJarInUsrLibDir() throws IOException, FlinkException {
        final File usrLibDir = temporaryFolder.newFolder("job");
        final File jarFileInUsrLibDir =
                Files.createFile(usrLibDir.toPath().resolve("jar_file_in_job_dir.jar")).toFile();
        final Path workingDirectory = FileUtils.getCurrentWorkingDirectory();
        final Path relativeJarFileInUsrLibDirPath =
                FileUtils.relativizePath(workingDirectory, jarFileInUsrLibDir.toPath());
        final List<URL> expectedURLs = new ArrayList<>();

        expectedURLs.add(jarFileInJobGraph.toUri().toURL());
        expectedURLs.add(FileUtils.toURL(relativeJarFileInUsrLibDirPath));

        final FileJobGraphRetriever fileJobGraphRetriever =
                FileJobGraphRetriever.createFrom(configuration, usrLibDir);
        final JobGraph jobGraphFromFile = fileJobGraphRetriever.retrieveJobGraph(configuration);

        assertThat(jobGraphFromFile.getClasspaths(), containsInAnyOrder(expectedURLs.toArray()));
    }

    @Test
    public void testRetrieveJobGraphWithoutUsrLibDir() throws IOException, FlinkException {
        final FileJobGraphRetriever fileJobGraphRetriever =
                FileJobGraphRetriever.createFrom(configuration, null);
        final List<URL> expectedUrls = Collections.singletonList(jarFileInJobGraph.toUri().toURL());
        final JobGraph jobGraph = fileJobGraphRetriever.retrieveJobGraph(configuration);

        assertThat(jobGraph.getClasspaths(), containsInAnyOrder(expectedUrls.toArray()));
    }
}
