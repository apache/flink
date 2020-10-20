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

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.CatalogTest;
import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.util.StringUtils;

import com.klarna.hiverunner.HiveShell;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

	private static final byte[] SEPARATORS = new byte[]{(byte) 1, (byte) 2, (byte) 3};

	/**
	 * Create a HiveCatalog with an embedded Hive Metastore.
	 */
	public static HiveCatalog createHiveCatalog() {
		return createHiveCatalog(CatalogTest.TEST_CATALOG_NAME, null);
	}

	public static HiveCatalog createHiveCatalog(String name, String hiveVersion) {
		return new HiveCatalog(name, null, createHiveConf(),
				StringUtils.isNullOrWhitespaceOnly(hiveVersion) ? HiveShimLoader.getHiveVersion() : hiveVersion, true);
	}

	public static HiveCatalog createHiveCatalog(String name, String hiveConfDir, String hadoopConfDir, String hiveVersion) {
		return new HiveCatalog(name, null, hiveConfDir, hadoopConfDir,
			StringUtils.isNullOrWhitespaceOnly(hiveVersion) ? HiveShimLoader.getHiveVersion() : hiveVersion);
	}

	public static HiveCatalog createHiveCatalog(HiveConf hiveConf) {
		return new HiveCatalog(CatalogTest.TEST_CATALOG_NAME, null, hiveConf, HiveShimLoader.getHiveVersion(), true);
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

	public static TableEnvironment createTableEnvWithBlinkPlannerBatchMode() {
		return createTableEnvWithBlinkPlannerBatchMode(SqlDialect.DEFAULT);
	}

	public static TableEnvironment createTableEnvWithBlinkPlannerBatchMode(SqlDialect dialect) {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
		TableEnvironment tableEnv = TableEnvironment.create(settings);
		tableEnv.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);
		tableEnv.getConfig().setSqlDialect(dialect);
		return tableEnv;
	}

	public static StreamTableEnvironment createTableEnvWithBlinkPlannerStreamMode(
			StreamExecutionEnvironment env) {
		return createTableEnvWithBlinkPlannerStreamMode(env, SqlDialect.DEFAULT);
	}

	public static StreamTableEnvironment createTableEnvWithBlinkPlannerStreamMode(
			StreamExecutionEnvironment env, SqlDialect dialect) {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
		tableEnv.getConfig().getConfiguration().setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key(), 1);
		tableEnv.getConfig().setSqlDialect(dialect);
		return tableEnv;
	}

	public static TableEnvironment createTableEnvWithHiveCatalog(HiveCatalog catalog) {
		TableEnvironment tableEnv = HiveTestUtils.createTableEnvWithBlinkPlannerBatchMode();
		tableEnv.registerCatalog(catalog.getName(), catalog);
		tableEnv.useCatalog(catalog.getName());
		return tableEnv;
	}

	// Insert into a single partition of a text table.
	public static TextTableInserter createTextTableInserter(HiveShell hiveShell, String dbName, String tableName) {
		return new TextTableInserter(hiveShell, dbName, tableName);
	}

	/**
	 * insert table operation.
	 */
	public static class TextTableInserter {

		private final HiveShell hiveShell;
		private final String dbName;
		private final String tableName;
		private final List<Object[]> rows;

		public TextTableInserter(HiveShell hiveShell, String dbName, String tableName) {
			this.hiveShell = hiveShell;
			this.dbName = dbName;
			this.tableName = tableName;
			rows = new ArrayList<>();
		}

		public TextTableInserter addRow(Object[] row) {
			rows.add(row);
			return this;
		}

		public void commit() {
			commit(null);
		}

		public void commit(String partitionSpec) {
			try {
				File file = File.createTempFile("table_data_", null);
				try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
					for (int i = 0; i < rows.size(); i++) {
						if (i > 0) {
							writer.newLine();
						}
						writer.write(toText(rows.get(i)));
					}
					// new line at the end of file
					writer.newLine();
				}
				String load = String.format("load data local inpath '%s' into table %s.%s", file.getAbsolutePath(), dbName, tableName);
				if (partitionSpec != null) {
					load += String.format(" partition (%s)", partitionSpec);
				}
				hiveShell.execute(load);
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		private String toText(Object[] row) {
			StringBuilder builder = new StringBuilder();
			for (Object col : row) {
				if (builder.length() > 0) {
					builder.appendCodePoint(SEPARATORS[0]);
				}
				String colStr = toText(col);
				if (colStr != null) {
					builder.append(colStr);
				}
			}
			return builder.toString();
		}

		private String toText(Object obj) {
			if (obj == null) {
				return null;
			}
			StringBuilder builder = new StringBuilder();
			if (obj instanceof Map) {
				for (Object key : ((Map) obj).keySet()) {
					if (builder.length() > 0) {
						builder.appendCodePoint(SEPARATORS[1]);
					}
					builder.append(toText(key));
					builder.appendCodePoint(SEPARATORS[2]);
					builder.append(toText(((Map) obj).get(key)));
				}
			} else if (obj instanceof Object[]) {
				Object[] array = (Object[]) obj;
				for (Object element : array) {
					if (builder.length() > 0) {
						builder.appendCodePoint(SEPARATORS[1]);
					}
					builder.append(toText(element));
				}
			} else if (obj instanceof List) {
				for (Object element : (List) obj) {
					if (builder.length() > 0) {
						builder.appendCodePoint(SEPARATORS[1]);
					}
					builder.append(toText(element));
				}
			} else {
				builder.append(obj);
			}
			return builder.toString();
		}
	}
}
