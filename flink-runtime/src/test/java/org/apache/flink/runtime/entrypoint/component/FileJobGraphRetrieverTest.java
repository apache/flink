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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.FlinkException;

import org.apache.commons.collections.CollectionUtils;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever.JOB_GRAPH_FILE_PATH;
import static org.junit.Assert.assertTrue;



/**
 * Tests for the {@link FileJobGraphRetriever}.
 */
public class FileJobGraphRetrieverTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static final TemporaryFolder GOBGRAPH_FOLDER = new TemporaryFolder();

	private static Path jarFileInJobGraph;
	private static Configuration configuration;

	@BeforeClass
	public static void init() throws IOException {
		final Path jobGraphPath = GOBGRAPH_FOLDER.newFile(JOB_GRAPH_FILE_PATH.defaultValue()).toPath();
		jarFileInJobGraph = GOBGRAPH_FOLDER.newFile("jar_file_in_job_graph.jar").toPath();

		final JobVertex source = new JobVertex("source");
		final JobVertex target = new JobVertex("target");
		final JobGraph jobGraph = new JobGraph(new JobID(), "test", source, target);

		jobGraph.setClasspaths(Arrays.asList(jarFileInJobGraph.toUri().toURL()));

		try (ObjectOutputStream objectOutputStream =
				new ObjectOutputStream(Files.newOutputStream(jobGraphPath, CREATE))) {
			objectOutputStream.writeObject(jobGraph);
		}

		configuration = new Configuration();
		configuration.setString(JOB_GRAPH_FILE_PATH.key(), jobGraphPath.toString());
	}

	@Test
	public void testRetrieveJobGraphWithJarInJobDir() throws IOException, FlinkException {
		final File jobDir = temporaryFolder.newFolder("job");
		final File jarFileInJobDir = Files.createFile(jobDir.toPath().resolve("jar_file_in_job_dir.jar")).toFile();

		final Collection<File> relativeFiles = FileUtils.relativizeToWorkingDir(Arrays.asList(jarFileInJobDir));
		final Collection<URL> relativeURLs = FileUtils.toRelativeURLs(relativeFiles);
		final List<URL> expectedURLs = new LinkedList<>();
		expectedURLs.add(jarFileInJobGraph.toUri().toURL());
		expectedURLs.addAll(relativeURLs);

		final FileJobGraphRetriever fileJobGraphRetriever = FileJobGraphRetriever.createFrom(configuration, jobDir);
		final JobGraph jobGraphFromFile = fileJobGraphRetriever.retrieveJobGraph(configuration);

		assertTrue(CollectionUtils.isEqualCollection(expectedURLs, jobGraphFromFile.getClasspaths()));
	}

	@Test
	public void testRetrieveJobGraphWithOutJobDir() throws IOException, FlinkException {
		final FileJobGraphRetriever fileJobGraphRetriever = FileJobGraphRetriever.createFrom(configuration);
		final List<URL> expectedUrls = Arrays.asList(jarFileInJobGraph.toUri().toURL());
		final JobGraph jobGraph = fileJobGraphRetriever.retrieveJobGraph(configuration);

		assertTrue(CollectionUtils.isEqualCollection(expectedUrls, jobGraph.getClasspaths()));
	}

}
