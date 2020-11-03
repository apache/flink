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

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.scala.DataStream;
import org.apache.flink.table.planner.runtime.utils.ParallelFiniteTestSource;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.flink.table.filesystem.stream.compact.CompactOperator.COMPACTED_PREFIX;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Streaming sink File Compaction ITCase base, test checkpoint.
 */
public abstract class FileCompactionITCaseBase extends StreamingTestBase {

	@Rule
	public Timeout timeoutPerTest = Timeout.seconds(60);

	private String resultPath;

	private List<Row> expectedRows;

	@Before
	public void init() throws IOException {
		resultPath = tempFolder().newFolder().toURI().toString();

		env().setParallelism(3);
		env().enableCheckpointing(100);

		List<Row> rows = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			rows.add(Row.of(i, String.valueOf(i % 10), String.valueOf(i)));
		}

		this.expectedRows = new ArrayList<>();
		this.expectedRows.addAll(rows);
		this.expectedRows.addAll(rows);
		this.expectedRows.sort(Comparator.comparingInt(o -> (Integer) o.getField(0)));

		DataStream<Row> stream = new DataStream<>(env().getJavaEnv().addSource(
				new ParallelFiniteTestSource<>(rows),
				new RowTypeInfo(
						new TypeInformation[] {Types.INT, Types.STRING, Types.STRING},
						new String[] {"a", "b", "c"})));

		tEnv().createTemporaryView("my_table",  stream);
	}

	protected abstract String format();

	@Test
	public void testNonPartition() throws Exception {
		tEnv().executeSql("CREATE TABLE sink_table (a int, b string, c string) with (" + options() + ")");
		tEnv().executeSql("insert into sink_table select * from my_table").await();

		assertIterator(tEnv().executeSql("select * from sink_table").collect());

		assertFiles(new File(URI.create(resultPath)).listFiles(), false);
	}

	@Test
	public void testPartition() throws Exception {
		tEnv().executeSql("CREATE TABLE sink_table (a int, b string, c string) partitioned by (b) with (" + options() + ")");
		tEnv().executeSql("insert into sink_table select * from my_table").await();

		assertIterator(tEnv().executeSql("select * from sink_table").collect());

		File path = new File(URI.create(resultPath));
		assertEquals(10, path.listFiles().length);

		for (int i = 0; i < 10; i++) {
			File partition = new File(path, "b=" + i);
			assertFiles(partition.listFiles(), true);
		}
	}

	private String options() {
		return "'connector'='filesystem'," +
				"'sink.partition-commit.policy.kind'='success-file'," +
				"'auto-compaction'='true'," +
				"'compaction.file-size' = '128MB'," +
				"'sink.rolling-policy.file-size' = '1b'," + // produce multiple files per task
				kv("format", format()) + "," +
				kv("path", resultPath);
	}

	private String kv(String key, String value) {
		return String.format("'%s'='%s'", key, value);
	}

	private void assertIterator(CloseableIterator<Row> iterator) throws Exception {
		List<Row> result = CollectionUtil.iteratorToList(iterator);
		iterator.close();
		result.sort(Comparator.comparingInt(o -> (Integer) o.getField(0)));
		assertEquals(expectedRows, result);
	}

	private void assertFiles(File[] files, boolean containSuccess) {
		File successFile = null;
		for (File file : files) {
			if (containSuccess && file.getName().equals("_SUCCESS")) {
				successFile = file;
			} else {
				assertTrue(file.getName(), file.getName().startsWith(COMPACTED_PREFIX));
			}
		}
		if (containSuccess) {
			Assert.assertNotNull("Should contains success file", successFile);
		}
	}
}
