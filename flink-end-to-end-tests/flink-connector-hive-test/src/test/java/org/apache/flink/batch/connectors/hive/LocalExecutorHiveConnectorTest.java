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

package org.apache.flink.batch.connectors.hive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.cli.util.DummyCustomCommandLine;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientFactory;
import org.apache.flink.table.catalog.hive.client.HiveMetastoreClientWrapper;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.local.LocalExecutor;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.FileUtils;

import com.klarna.hiverunner.HiveShell;
import com.klarna.hiverunner.annotations.HiveSQL;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests Hive connector with LocalExecutor.
 */
@RunWith(FlinkStandaloneHiveRunner.class)
public class LocalExecutorHiveConnectorTest {

	private static final int NUM_TMS = 2;
	private static final int NUM_SLOTS_PER_TM = 2;

	private static Executor executor;
	private static SessionContext session;
	private static HiveMetastoreClientWrapper hmsClient;

	private static final String HIVE_CATALOG_YAML_FILE = "test-sql-client-hive.yaml";

	@HiveSQL(files = {})
	private static HiveShell hiveShell;

	@ClassRule
	public static final MiniClusterWithClientResource MINI_CLUSTER_RESOURCE = new MiniClusterWithClientResource(
			new MiniClusterResourceConfiguration.Builder()
					.setConfiguration(getConfig())
					.setNumberTaskManagers(NUM_TMS)
					.setNumberSlotsPerTaskManager(NUM_SLOTS_PER_TM)
					.build());

	@BeforeClass
	public static void setup() throws Exception {
		executor = createHiveExecutor(MINI_CLUSTER_RESOURCE.getClusterClient());
		session = new SessionContext("test-session", new Environment());

		hmsClient = HiveMetastoreClientFactory.create(hiveShell.getHiveConf(), null);
	}

	@Test
	public void testWriteHiveTable() throws Exception {

		// primitive types
		hiveShell.execute("create database db1");
		hiveShell.execute("create table db1.simple (i int, s string)");
		waitForJobFinish(executor.executeUpdate(session, "insert into db1.simple values (1,'a')"));
		assertEquals(Collections.singletonList("1\ta"), hiveShell.executeQuery("select * from db1.simple"));

		hiveShell.execute("drop database db1 cascade");
	}

	private static <T> LocalExecutor createHiveExecutor(ClusterClient<T> clusterClient) throws IOException {
		File tempConfDir = Files.createTempDirectory("temp_hive_conf_dir").toFile();
		Runtime.getRuntime().addShutdownHook(new Thread(() -> FileUtils.deleteDirectoryQuietly(tempConfDir)));
		File hiveSite = new File(tempConfDir, "hive-site.xml");
		try (FileOutputStream outputStream = new FileOutputStream(hiveSite)) {
			hiveShell.getHiveConf().writeXml(outputStream);
		}
		Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_HIVE_CONF_DIR", tempConfDir.toString());
		Environment environment = EnvironmentFileUtil.parseModified(HIVE_CATALOG_YAML_FILE, replaceVars);

		assertEquals("Num of catalog mismatch", 1, environment.getCatalogs().size());

		return new LocalExecutor(environment,
				Collections.emptyList(),
				clusterClient.getFlinkConfiguration(),
				new DummyCustomCommandLine<T>(clusterClient));
	}

	private static Configuration getConfig() {
		Configuration config = new Configuration();
		config.setString(TaskManagerOptions.MANAGED_MEMORY_SIZE, "4m");
		config.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, NUM_TMS);
		config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, NUM_SLOTS_PER_TM);
		config.setBoolean(WebOptions.SUBMIT_ENABLE, false);
		return config;
	}

	private static void waitForJobFinish(ProgramTargetDescriptor targetDescriptor) throws Exception {
		boolean isRunning = true;
		while (isRunning) {
			Thread.sleep(50); // slow the processing down
			final JobStatus jobStatus = MINI_CLUSTER_RESOURCE.getClusterClient().getJobStatus(JobID.fromHexString(targetDescriptor.getJobId())).get();
			switch (jobStatus) {
				case CREATED:
				case RUNNING:
					continue;
				case FINISHED:
					isRunning = false;
					break;
				default:
					fail("Unexpected job status.");
			}
		}
	}
}
