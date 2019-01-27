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

package org.apache.flink.orc;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBase;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for {@link OrcTableSource}.
 */
@Ignore //Ignore for DataSet test cases.
public class OrcTableSourceITCase extends MultipleProgramsTestBase {

	private static final String TEST_FILE_FLAT = "test-data-flat.orc";
	private static final String TEST_SCHEMA_FLAT =
		"struct<_col0:int,_col1:string,_col2:string,_col3:string,_col4:int,_col5:string,_col6:int,_col7:int,_col8:int>";

	public OrcTableSourceITCase() {
		super(TestExecutionMode.COLLECTION);
	}

	@Test
	public void testFullScan() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env);

		OrcTableSource orc = OrcTableSource.builder()
			.path(getPath(TEST_FILE_FLAT))
			.forOrcSchema(TEST_SCHEMA_FLAT)
			.build();
		tEnv.registerTableSource("OrcTable", orc);

		String query =
			"SELECT COUNT(*), " +
				"MIN(_col0), MAX(_col0), " +
				"MIN(_col1), MAX(_col1), " +
				"MIN(_col2), MAX(_col2), " +
				"MIN(_col3), MAX(_col3), " +
				"MIN(_col4), MAX(_col4), " +
				"MIN(_col5), MAX(_col5), " +
				"MIN(_col6), MAX(_col6), " +
				"MIN(_col7), MAX(_col7), " +
				"MIN(_col8), MAX(_col8) " +
			"FROM OrcTable";
		Table t = tEnv.sqlQuery(query);

		t.print();
	}

	@Test
	public void testScanWithProjectionAndFilter() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tEnv = TableEnvironment.getBatchTableEnvironment(env);

		OrcTableSource orc = OrcTableSource.builder()
			.path(getPath(TEST_FILE_FLAT))
			.forOrcSchema(TEST_SCHEMA_FLAT)
			.build();
		tEnv.registerTableSource("OrcTable", orc);

		String query =
			"SELECT " +
				"MIN(_col4), MAX(_col4), " +
				"MIN(_col3), MAX(_col3), " +
				"MIN(_col0), MAX(_col0), " +
				"MIN(_col2), MAX(_col2), " +
				"COUNT(*) " +
				"FROM OrcTable " +
				"WHERE (_col0 BETWEEN 4975 and 5024 OR _col0 BETWEEN 9975 AND 10024) AND _col1 = 'F'";
		Table t = tEnv.sqlQuery(query);

		t.print();
	}

	private String getPath(String fileName) {
		return getClass().getClassLoader().getResource(fileName).getPath();
	}
}
