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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.CatalogTest;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.util.StringUtils;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM;

/**
 * Test utils for Hive connector.
 */
public class HiveTestUtils {
	private static final String HIVE_SITE_XML = "hive-site.xml";
	private static final String HIVE_WAREHOUSE_URI_FORMAT = "jdbc:derby:;databaseName=%s;create=true";
	private static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	// range of ephemeral ports
	private static final int MIN_EPH_PORT = 49152;
	private static final int MAX_EPH_PORT = 61000;

	/**
	 * Create a HiveCatalog with an embedded Hive Metastore.
	 */
	public static HiveCatalog createHiveCatalog() {
		return createHiveCatalog(CatalogTest.TEST_CATALOG_NAME, null);
	}

	public static HiveCatalog createHiveCatalog(String name, String hiveVersion) {
		return new HiveCatalog(name, null, createHiveConf(),
				StringUtils.isNullOrWhitespaceOnly(hiveVersion) ? HiveShimLoader.getHiveVersion() : hiveVersion);
	}

	public static HiveCatalog createHiveCatalog(HiveConf hiveConf) {
		return new HiveCatalog(CatalogTest.TEST_CATALOG_NAME, null, hiveConf, HiveShimLoader.getHiveVersion());
	}

	public static HiveConf createHiveConf() {
		ClassLoader classLoader = new HiveTestUtils().getClass().getClassLoader();
		HiveConf.setHiveSiteLocation(classLoader.getResource(HIVE_SITE_XML));

		try {
			TEMPORARY_FOLDER.create();
			String warehouseDir = TEMPORARY_FOLDER.newFolder().getAbsolutePath() + "/metastore_db";
			String warehouseUri = String.format(HIVE_WAREHOUSE_URI_FORMAT, warehouseDir);

			HiveConf hiveConf = new HiveConf();
			hiveConf.setVar(HiveConf.ConfVars.METASTOREWAREHOUSE, TEMPORARY_FOLDER.newFolder("hive_warehouse").getAbsolutePath());
			hiveConf.setVar(HiveConf.ConfVars.METASTORECONNECTURLKEY, warehouseUri);
			return hiveConf;
		} catch (IOException e) {
			throw new CatalogException(
				"Failed to create test HiveConf to HiveCatalog.", e);
		}
	}

	// Gets a free port of localhost. Note that this method suffers the "time of check to time of use" race condition.
	// Use it as best efforts to avoid port conflicts.
	public static int getFreePort() throws IOException {
		final int numPorts = MAX_EPH_PORT - MIN_EPH_PORT + 1;
		int numAttempt = 0;
		while (numAttempt++ < numPorts) {
			int p = ThreadLocalRandom.current().nextInt(numPorts) + MIN_EPH_PORT;
			try (ServerSocket socket = new ServerSocket()) {
				socket.bind(new InetSocketAddress("localhost", p));
				return socket.getLocalPort();
			} catch (BindException e) {
				// this port is in use, try another one
			}
		}
		throw new RuntimeException("Exhausted all ephemeral ports and didn't find a free one");
	}

	public static TableEnvironment createTableEnv() {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);
		tableEnv.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);
		return tableEnv;
	}
}
