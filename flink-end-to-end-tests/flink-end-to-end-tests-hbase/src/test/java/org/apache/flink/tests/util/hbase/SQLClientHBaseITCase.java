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

package org.apache.flink.tests.util.hbase;

import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.cache.DownloadCache;
import org.apache.flink.tests.util.categories.PreCommit;
import org.apache.flink.tests.util.categories.TravisGroup1;
import org.apache.flink.tests.util.flink.ClusterController;
import org.apache.flink.tests.util.flink.FlinkResource;
import org.apache.flink.tests.util.flink.FlinkResourceSetup;
import org.apache.flink.tests.util.flink.LocalStandaloneFlinkResourceFactory;
import org.apache.flink.tests.util.flink.SQLJobSubmission;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.base.Charsets;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

/**
 * End-to-end test for the HBase connectors.
 */
@Category(value = {TravisGroup1.class, PreCommit.class})
public class SQLClientHBaseITCase extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(SQLClientHBaseITCase.class);

	private static final String HBASE_SOURCE_SINK_SCHEMA = "hbase_source_sink_schema.yaml";

	@Rule
	public final HBaseResource hbase;

	@Rule
	public final FlinkResource flink = new LocalStandaloneFlinkResourceFactory()
		.create(FlinkResourceSetup.builder().build());

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@ClassRule
	public static final DownloadCache DOWNLOAD_CACHE = DownloadCache.get();

	private static final Path sqlToolBoxJar = TestUtils.getResourceJar(".*SqlToolbox.jar");
	private static final Path sqlConnectorHBaseJar = TestUtils.getResourceJar(".*SqlConnectorHbase.jar");
	private Path shadedHadoopJar;
	private Path sqlClientSessionConf;

	public SQLClientHBaseITCase() {
		this.hbase = HBaseResource.get();
	}

	@Before
	public void before() throws Exception {
		DOWNLOAD_CACHE.before();
		Path tmpPath = tmp.getRoot().toPath();
		LOG.info("The current temporary path: {}", tmpPath);
		this.sqlClientSessionConf = tmpPath.resolve("sql-client-session.conf");

		//download flink shaded hadoop jar for hbase e2e test using
		shadedHadoopJar = DOWNLOAD_CACHE.getOrDownload(
			"https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.4.1-10.0/flink-shaded-hadoop-2-uber-2.4.1-10.0.jar", tmpPath);
	}

	@Test
	public void testHBase() throws Exception {
		try (ClusterController clusterController = flink.startCluster(2)) {
			// Create table and put data
			hbase.createTable("source_table", "a", "b");
			hbase.createTable("sink_table", "a", "b");
			hbase.putData("source_table", "row1", "a", "c1", "v1");
			hbase.putData("source_table", "row1", "b", "c1", "v2");
			hbase.putData("source_table", "row2", "a", "c1", "v3");
			hbase.putData("source_table", "row2", "b", "c1", "v4");

			// Initialize the SQL client session configuration file
			String schemaContent = initializeSessionYaml();
			Files.write(this.sqlClientSessionConf,
				schemaContent.getBytes(Charsets.UTF_8),
				StandardOpenOption.CREATE,
				StandardOpenOption.WRITE);

			// Executing SQL: read the data from HBase source_table and then write into HBase sink_table
			insertIntoHBaseTable(clusterController);

			LOG.info("Verify the sink_table result.");
			final List<String> result = hbase.scanTable("sink_table");
			assertThat(
				result.toArray(new String[0]),
				arrayContainingInAnyOrder(
					containsString("value=v1"),
					containsString("value=v2"),
					containsString("value=v3"),
					containsString("value=v4")
				));
			LOG.info("The HBase SQL client test run successfully.");
		}

	}

	private String initializeSessionYaml() throws IOException {
		URL url = SQLClientHBaseITCase.class.getClassLoader().getResource(HBASE_SOURCE_SINK_SCHEMA);
		if (url == null) {
			throw new FileNotFoundException(HBASE_SOURCE_SINK_SCHEMA);
		}

		return FileUtils.readFileUtf8(new File(url.getFile()));
	}

	private void insertIntoHBaseTable(ClusterController clusterController) throws IOException {
		LOG.info("Executing SQL: HBase source_table -> HBase sink_table");
		String sqlStatement = "INSERT INTO MyHBaseSink\n" +
			"	SELECT rowkey, a, b from MyHBaseSource";

		clusterController.submitSQLJob(new SQLJobSubmission.SQLJobSubmissionBuilder(sqlStatement)
			.addJar(sqlToolBoxJar)
			.addJar(shadedHadoopJar)
			.addJar(sqlConnectorHBaseJar)
			.setSessionEnvFile(this.sqlClientSessionConf.toAbsolutePath().toString())
			.build());
	}
}
