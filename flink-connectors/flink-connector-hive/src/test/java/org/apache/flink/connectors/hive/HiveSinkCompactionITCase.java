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

package org.apache.flink.connectors.hive;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.catalog.hive.HiveTestUtils;
import org.apache.flink.table.planner.runtime.stream.sql.CompactionITCaseBase;

import org.junit.After;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/**
 * Test sink file compaction of hive tables.
 */
@RunWith(Parameterized.class)
public class HiveSinkCompactionITCase extends CompactionITCaseBase {

	@Parameterized.Parameters(name = "format = {0}")
	public static Collection<String> parameters() {
		return Arrays.asList("sequencefile", "parquet");
	}

	@Parameterized.Parameter
	public String format;

	private HiveCatalog hiveCatalog;

	@Override
	@Before
	public void init() throws IOException {
		hiveCatalog = HiveTestUtils.createHiveCatalog();
		tEnv().registerCatalog(hiveCatalog.getName(), hiveCatalog);
		tEnv().useCatalog(hiveCatalog.getName());

		// avoid too large parallelism lead to scheduler dead lock in streaming mode
		tEnv().getConfig().getConfiguration().set(
				HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM, false);

		super.init();
	}

	@After
	public void tearDown() throws TableNotExistException {
		if (hiveCatalog != null) {
			hiveCatalog.dropTable(new ObjectPath(tEnv().getCurrentDatabase(), "sink_table"), true);
			hiveCatalog.close();
		}
	}

	private void create(String path, boolean part) {
		tEnv().getConfig().setSqlDialect(SqlDialect.HIVE);
		tEnv().executeSql("CREATE TABLE sink_table (a int, b string" +
				(part ? "" : ",c string") + ") " +
				(part ? "partitioned by (c string) " : "") +
				" stored as " + format +
				" location '" + path + "'" +
				" TBLPROPERTIES (" +
				"'sink.partition-commit.policy.kind'='metastore,success-file'," +
				"'auto-compaction'='true'," +
				"'compaction.file-size' = '128MB'," +
				"'sink.rolling-policy.file-size' = '1b'" +
				")");
		tEnv().getConfig().setSqlDialect(SqlDialect.DEFAULT);
	}

	@Override
	protected String partitionField() {
		return "c";
	}

	@Override
	protected void createTable(String path) {
		create(path, false);
	}

	@Override
	protected void createPartitionTable(String path) {
		create(path, true);
	}
}
