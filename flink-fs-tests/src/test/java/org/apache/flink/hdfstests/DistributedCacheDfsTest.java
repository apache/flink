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

package org.apache.flink.hdfstests;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for distributing files with {@link org.apache.flink.api.common.cache.DistributedCache} via HDFS.
 */
public class DistributedCacheDfsTest extends TestLogger {

	private static final String testFileContent = "Goethe - Faust: Der Tragoedie erster Teil\n" + "Prolog im Himmel.\n"
		+ "Der Herr. Die himmlischen Heerscharen. Nachher Mephistopheles. Die drei\n" + "Erzengel treten vor.\n"
		+ "RAPHAEL: Die Sonne toent, nach alter Weise, In Brudersphaeren Wettgesang,\n"
		+ "Und ihre vorgeschriebne Reise Vollendet sie mit Donnergang. Ihr Anblick\n"
		+ "gibt den Engeln Staerke, Wenn keiner Sie ergruenden mag; die unbegreiflich\n"
		+ "hohen Werke Sind herrlich wie am ersten Tag.\n"
		+ "GABRIEL: Und schnell und unbegreiflich schnelle Dreht sich umher der Erde\n"
		+ "Pracht; Es wechselt Paradieseshelle Mit tiefer, schauervoller Nacht. Es\n"
		+ "schaeumt das Meer in breiten Fluessen Am tiefen Grund der Felsen auf, Und\n"
		+ "Fels und Meer wird fortgerissen Im ewig schnellem Sphaerenlauf.\n"
		+ "MICHAEL: Und Stuerme brausen um die Wette Vom Meer aufs Land, vom Land\n"
		+ "aufs Meer, und bilden wuetend eine Kette Der tiefsten Wirkung rings umher.\n"
		+ "Da flammt ein blitzendes Verheeren Dem Pfade vor des Donnerschlags. Doch\n"
		+ "deine Boten, Herr, verehren Das sanfte Wandeln deines Tags.";

	@ClassRule
	public static final TemporaryFolder TEMP_FOLDER = new TemporaryFolder();

	@ClassRule
	public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE = new MiniClusterWithClientResource(
		new MiniClusterResourceConfiguration.Builder()
			.setNumberTaskManagers(1)
			.setNumberSlotsPerTaskManager(1)
			.build());

	private static MiniDFSCluster hdfsCluster;
	private static Configuration conf = new Configuration();

	private static Path testFile;
	private static Path testDir;

	@BeforeClass
	public static void setup() throws Exception {
		File dataDir = TEMP_FOLDER.newFolder();

		conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, dataDir.getAbsolutePath());
		MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
		hdfsCluster = builder.build();

		String hdfsURI = "hdfs://"
			+ NetUtils.hostAndPortToUrlString(hdfsCluster.getURI().getHost(), hdfsCluster.getNameNodePort())
			+ "/";

		FileSystem dfs = FileSystem.get(new URI(hdfsURI));
		testFile = writeFile(dfs, dfs.getHomeDirectory(), "testFile");

		testDir = new Path(dfs.getHomeDirectory(), "testDir");
		dfs.mkdirs(testDir);
		writeFile(dfs, testDir, "testFile1");
		writeFile(dfs, testDir, "testFile2");
	}

	private static Path writeFile(FileSystem dfs, Path rootDir, String fileName) throws IOException {
		Path file = new Path(rootDir, fileName);
		try (
			DataOutputStream outStream = new DataOutputStream(dfs.create(file,
				FileSystem.WriteMode.OVERWRITE))) {
			outStream.writeUTF(testFileContent);
		}
		return file;
	}

	@AfterClass
	public static void teardown() {
		hdfsCluster.shutdown();
	}

	@Test
	public void testDistributedFileViaDFS() throws Exception {
		createJobWithRegisteredCachedFiles().execute("Distributed Cache Via Blob Test Program");
	}

	/**
	 * All the Flink Standalone, Yarn, Mesos, Kubernetes sessions are using {@link RestClusterClient#submitJob(JobGraph)}
	 * to submit a job to an existing session. This test will cover this cases.
	 */
	@Test(timeout = 30000)
	public void testSubmittingJobViaRestClusterClient() throws Exception {
		RestClusterClient<String> restClusterClient = new RestClusterClient<>(
			MINI_CLUSTER_RESOURCE.getClientConfiguration(),
			"testSubmittingJobViaRestClusterClient");

		final JobGraph jobGraph = createJobWithRegisteredCachedFiles()
				.getStreamGraph()
				.getJobGraph();

		final JobResult jobResult = restClusterClient
			.submitJob(jobGraph)
			.thenCompose(restClusterClient::requestJobResult)
			.get();

		final String messageInCaseOfFailure = jobResult.getSerializedThrowable().isPresent() ?
				jobResult.getSerializedThrowable().get().getFullStringifiedStackTrace()
				: "Job failed.";
		assertTrue(messageInCaseOfFailure, jobResult.isSuccess());
	}

	private StreamExecutionEnvironment createJobWithRegisteredCachedFiles() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env.registerCachedFile(testFile.toString(), "test_data", false);
		env.registerCachedFile(testDir.toString(), "test_dir", false);

		env.fromElements(1)
			.map(new TestMapFunction())
			.addSink(new DiscardingSink<>());
		return env;
	}

	private static class TestMapFunction extends RichMapFunction<Integer, String> {

		private static final long serialVersionUID = -3917258280687242969L;

		@Override
		public String map(Integer value) throws Exception {
			final Path actualFile = new Path(getRuntimeContext().getDistributedCache().getFile("test_data").toURI());

			Path path = new Path(actualFile.toUri());
			assertFalse(path.getFileSystem().isDistributedFS());

			DataInputStream in = new DataInputStream(actualFile.getFileSystem().open(actualFile));
			String contents = in.readUTF();

			assertEquals(testFileContent, contents);

			final Path actualDir = new Path(getRuntimeContext().getDistributedCache().getFile("test_dir").toURI());
			FileStatus fileStatus = actualDir.getFileSystem().getFileStatus(actualDir);
			assertTrue(fileStatus.isDir());
			FileStatus[] fileStatuses = actualDir.getFileSystem().listStatus(actualDir);
			assertEquals(2, fileStatuses.length);

			return contents;
		}
	}
}
