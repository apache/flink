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

package org.apache.flink.table.api;

import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.planner.runtime.utils.BatchTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.INT_TYPE_INFO;
import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.LONG_TYPE_INFO;
import static org.junit.Assert.assertEquals;

/**
 * IT case for {@link TableUtils} in batch mode.
 */
public class TableUtilsBatchITCase extends BatchTestBase {

	@Test
	public void testCollectToList() throws Exception {
		List<Row> sourceData = Arrays.asList(
			Row.of(1, 11L),
			Row.of(1, 12L),
			Row.of(2, 21L),
			Row.of(2, 22L),
			Row.of(3, 31L));
		registerJavaCollection(
			"T",
			sourceData,
			new RowTypeInfo(INT_TYPE_INFO, LONG_TYPE_INFO),
			"a, b");

		String sql = "SELECT sum(b) FROM T GROUP BY a HAVING a < 3 ORDER BY a";
		List<Row> expected = Arrays.asList(
			Row.of(23L),
			Row.of(43L));

		Table table = tEnv().sqlQuery(sql);
		// run multiple times to make sure no errors will occur
		// when the utility method is called a second time
		for (int i = 0; i < 2; i++) {
			List<Row> actual = TableUtils.collectToList(table);
			assertEquals(expected, actual);
		}
	}
}
