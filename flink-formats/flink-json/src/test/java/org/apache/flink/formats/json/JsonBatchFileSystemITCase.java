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

package org.apache.flink.formats.json;

import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.planner.runtime.batch.sql.BatchFileSystemITCaseBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.FileUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * ITCase to test json format for {@link JsonFormatFactory}.
 */
public class JsonBatchFileSystemITCase extends BatchFileSystemITCaseBase {

	@Override
	public String[] formatProperties() {
		List<String> ret = new ArrayList<>();
		ret.add("'format'='json'");
		ret.add("'json.ignore-parse-errors'='true'");
		return ret.toArray(new String[0]);
	}

	@Test
	public void testParseError() throws Exception {
		String path = new URI(resultPath()).getPath();
		new File(path).mkdirs();
		File file = new File(path, "temp_file");
		file.createNewFile();
		FileUtils.writeFileUtf8(file,
			"{\"x\":\"x5\",\"y\":5,\"a\":1,\"b\":1}\n" +
				"{I am a wrong json.}\n" +
				"{\"x\":\"x5\",\"y\":5,\"a\":1,\"b\":1}");

		check("select * from nonPartitionedTable",
			Arrays.asList(
				Row.of("x5,5,1,1"),
				Row.of("x5,5,1,1")));
	}

	@Test
	public void bigDataTest() throws IOException {
		int numRecords = 1000;
		File dir = generateTestData(numRecords);

		env().setParallelism(1);

		String sql = String.format(
				"CREATE TABLE bigdata_source ( " +
						"	id INT, " +
						"	content STRING" +
						") PARTITIONED by (id) WITH (" +
						"	'connector' = 'filesystem'," +
						"	'path' = '%s'," +
						"	'format' = 'json'" +
						")", dir);
		tEnv().executeSql(sql);
		TableResult result = tEnv().executeSql("select * from bigdata_source");
		List<String> elements = new ArrayList<>();
		result.collect().forEachRemaining(r -> elements.add((String) r.getField(1)));
		Assert.assertEquals(numRecords, elements.size());
		elements.sort(String::compareTo);

		List<String> expected = new ArrayList<>();
		for (int i = 0; i < numRecords; i++) {
			expected.add(String.valueOf(i));
		}
		expected.sort(String::compareTo);

		Assert.assertEquals(expected, elements);
	}

	private static File generateTestData(int numRecords) throws IOException {
		File tempDir = TEMPORARY_FOLDER.newFolder();

		File root = new File(tempDir, "id=0");
		root.mkdir();

		File dataFile = new File(root, "testdata");
		try (PrintWriter writer = new PrintWriter(dataFile)) {
			for (int i = 0; i < numRecords; ++i) {
				writer.println(String.format("{\"content\":\"%s\"}", i));
			}
		}

		return tempDir;
	}
}
