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
import org.apache.flink.util.FileUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Streaming sink File Compaction ITCase base, test checkpoint.
 */
public abstract class FileCompactionITCaseBase extends StreamingTestBase {

	@Rule
	public Timeout timeoutPerTest = Timeout.seconds(60);

	private String resultPath;

	private List<Row> rows;

	@Before
	public void init() throws IOException {
		resultPath = tempFolder().newFolder().toURI().toString();
		clear();

		env().setParallelism(3);
		env().enableCheckpointing(100);

		rows = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			rows.add(Row.of(i, String.valueOf(i % 10), String.valueOf(i)));
		}

		DataStream<Row> stream = new DataStream<>(env().getJavaEnv().addSource(
				new ParallelFiniteTestSource<>(rows),
				new RowTypeInfo(
						new TypeInformation[] {Types.INT, Types.STRING, Types.STRING},
						new String[] {"a", "b", "c"})));

		tEnv().createTemporaryView("my_table",  stream);
	}

	@After
	public void clear() throws IOException {
		FileUtils.deleteDirectory(new File(URI.create(resultPath)));
	}

	protected abstract String format();

	@Test
	public void testNonPartition() throws Exception {
		tEnv().executeSql("CREATE TABLE sink_table (a int, b string, c string) with (" + options() + ")");
		tEnv().executeSql("insert into sink_table select * from my_table").await();

		List<Row> results = toListAndClose(tEnv().executeSql("select * from sink_table").collect());
		results.sort(Comparator.comparingInt(o -> (Integer) o.getField(0)));
		assertEquals(rows, results);

		File[] files = new File(URI.create(resultPath)).listFiles(
				(dir, name) -> name.startsWith("compacted-part-"));
		assertEquals(Arrays.toString(files), 1, files.length);

		String fileName = files[0].getName();
		assertTrue(fileName, fileName.startsWith("compacted-part-"));
	}

	@Test
	public void testPartition() throws Exception {
		tEnv().executeSql("CREATE TABLE sink_table (a int, b string, c string) partitioned by (b) with (" + options() + ")");
		tEnv().executeSql("insert into sink_table select * from my_table").await();

		List<Row> results = toListAndClose(tEnv().executeSql("select * from sink_table").collect());
		results.sort(Comparator.comparingInt(o -> (Integer) o.getField(0)));
		assertEquals(rows, results);

		File path = new File(URI.create(resultPath));
		assertEquals(10, path.listFiles().length);

		for (int i = 0; i < 10; i++) {
			File partition = new File(path, "b=" + i);
			File[] files = partition.listFiles();
			assertEquals(Arrays.toString(files), 2, files.length);
			assertEquals(1, partition.list((dir, name) -> name.equals("_SUCCESS")).length);
			assertEquals(1, partition.list((dir, name) -> name.startsWith("compacted-part-")).length);
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

	private List<Row> toListAndClose(CloseableIterator<Row> iterator) throws Exception {
		List<Row> rows = CollectionUtil.iteratorToList(iterator);
		iterator.close();
		return rows;
	}
}
