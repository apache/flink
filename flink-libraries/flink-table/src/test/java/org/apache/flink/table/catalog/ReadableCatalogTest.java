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

package org.apache.flink.table.catalog;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.runtime.utils.CommonTestData;
import org.apache.flink.table.util.StreamTableTestUtil;
import org.apache.flink.table.util.TableTestBase;

import org.junit.Test;

/**
 * Test for ReadableCatalog.
 */
public class ReadableCatalogTest extends TableTestBase {
	@Test
	public void testStreamTableApi() {
		StreamTableTestUtil util = streamTestUtil();

		util.tableEnv().registerCatalog("test", CommonTestData.getTestFlinkInMemoryCatalog());

		Table t1 = util.tableEnv().scan("test", "db1", "tb1");
		Table t2 = util.tableEnv().scan("test", "db2", "tb2");

		Table result = t2.where("d < 3")
			.select("d * 2, e, g.upperCase()")
			.unionAll(t1.select("a * 2, b, c.upperCase()"));

		util.verifyPlan(result);
	}

	@Test
	public void testStreamSQL() {
		StreamTableTestUtil util = streamTestUtil();

		util.tableEnv().registerCatalog("test", CommonTestData.getTestFlinkInMemoryCatalog());

		String sqlQuery = "SELECT d * 2, e, g FROM test.db2.tb2 WHERE d < 3 UNION ALL " +
			"(SELECT a * 2, b, c FROM test.db1.tb1)";

		util.verifyPlan(sqlQuery);
	}
}
