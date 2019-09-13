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
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.FlinkException;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static org.junit.Assert.assertTrue;



/**
 * Tests for the {@link FileJobGraphRetriever}.
 */
public class FileJobGraphRetrieverTest {

	@Rule
	public final TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Path jobGraphPath;
	private Path jarFileInJobGraph;
	private Configuration configuration;

	@Before
	public void init() throws IOException {
		jobGraphPath = temporaryFolder.newFile("job.graph").toPath();
		jarFileInJobGraph = temporaryFolder.newFile("jar_file_in_job_graph.jar").toPath();
		JobVertex source = new JobVertex("source");
		JobVertex target = new JobVertex("target");
		target.connectNewDataSetAsInput(source, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
		JobGraph jobGraph = new JobGraph(new JobID(), "test", source, target);
		jobGraph.setClasspaths(Arrays.asList(jarFileInJobGraph.toUri().toURL()));

		ObjectOutputStream objectOutputStream = new ObjectOutputStream(Files.newOutputStream(jobGraphPath, CREATE));
		objectOutputStream.writeObject(jobGraph);
		configuration = new Configuration();
		configuration.setString(FileJobGraphRetriever.JOB_GRAPH_FILE_PATH.key(), jobGraphPath.toString());
	}

	@Test
	public void testRetrieveJobGraphWithJarInJobDir() throws IOException, FlinkException {

		Path jobDir = Paths.get("job");
		Path jarFileInJobDir = Paths.get(jobDir.toString(), "jar_file_in_job_dir.jar");

		final List<URL> expectedUrls = Arrays.asList(
			jarFileInJobGraph.toUri().toURL(),
			new URL(new URL(jarFileInJobDir.toUri().getScheme() + ":"), jarFileInJobDir.toString()));
		try {
			if (!Files.exists(jobDir)) {
				Files.createDirectory(jobDir);
			}
			if (!Files.exists(jarFileInJobDir)) {
				Files.createFile(jarFileInJobDir);
			}

			FileJobGraphRetriever fileJobGraphRetriever = FileJobGraphRetriever.createFrom(configuration);
			JobGraph jobGraphFromFile = fileJobGraphRetriever.retrieveJobGraph(configuration);
			assertTrue(CollectionUtils.isEqualCollection(expectedUrls, jobGraphFromFile.getClasspaths()));
		} finally {
			Files.deleteIfExists(jarFileInJobDir);
			Files.deleteIfExists(jobDir);
		}
	}

	@Test
	public void testRetrieveJobGraphWithOutJobDir() throws IOException, FlinkException {
		FileJobGraphRetriever fileJobGraphRetriever = FileJobGraphRetriever.createFrom(configuration);
		List<URL> expectedUrls = Arrays.asList(jarFileInJobGraph.toUri().toURL());
		JobGraph jobGraph = fileJobGraphRetriever.retrieveJobGraph(configuration);
		assertTrue(CollectionUtils.isEqualCollection(expectedUrls, jobGraph.getClasspaths()));
	}

}
