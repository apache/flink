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

package org.apache.flink.connectors.hive.write;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;
import org.apache.flink.table.filesystem.OutputFormatFactory;
import org.apache.flink.types.Row;

import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.function.Function;

/**
 * Hive {@link OutputFormatFactory}, use {@link RecordWriter} to write record.
 */
public class HiveOutputFormatFactory implements OutputFormatFactory<Row> {

	private static final long serialVersionUID = 2L;

	private final HiveWriterFactory factory;

	public HiveOutputFormatFactory(HiveWriterFactory factory) {
		this.factory = factory;
	}

	@Override
	public HiveOutputFormat createOutputFormat(Path path) {
		return new HiveOutputFormat(
				factory.createRecordWriter(HadoopFileSystem.toHadoopPath(path)),
				factory.createRowConverter());
	}

	private class HiveOutputFormat implements org.apache.flink.api.common.io.OutputFormat<Row> {

		private final RecordWriter recordWriter;
		private final Function<Row, Writable> rowConverter;

		private HiveOutputFormat(RecordWriter recordWriter, Function<Row, Writable> rowConverter) {
			this.recordWriter = recordWriter;
			this.rowConverter = rowConverter;
		}

		@Override
		public void configure(Configuration parameters) {
		}

		@Override
		public void open(int taskNumber, int numTasks) {
		}

		@Override
		public void writeRecord(Row record) throws IOException {
			recordWriter.write(rowConverter.apply(record));
		}

		@Override
		public void close() throws IOException {
			recordWriter.close(false);
		}
	}
}
