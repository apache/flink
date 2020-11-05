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

/**
 * Streaming sink File Compaction ITCase base for File system connector.
 */
public abstract class FileCompactionITCaseBase extends CompactionITCaseBase {

	protected abstract String format();

	@Override
	protected String partitionField() {
		return "b";
	}

	@Override
	protected void createTable(String path) {
		tEnv().executeSql("CREATE TABLE sink_table (a int, b string, c string) with (" + options(path) + ")");
	}

	@Override
	protected void createPartitionTable(String path) {
		tEnv().executeSql("CREATE TABLE sink_table (a int, b string, c string) partitioned by (b) with (" + options(path) + ")");
	}

	private String options(String path) {
		return "'connector'='filesystem'," +
				"'sink.partition-commit.policy.kind'='success-file'," +
				"'auto-compaction'='true'," +
				"'compaction.file-size' = '128MB'," +
				"'sink.rolling-policy.file-size' = '1b'," + // produce multiple files per task
				kv("format", format()) + "," +
				kv("path", path);
	}

	private String kv(String key, String value) {
		return String.format("'%s'='%s'", key, value);
	}
}
