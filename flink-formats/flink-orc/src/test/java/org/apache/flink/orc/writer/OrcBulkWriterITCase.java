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

package org.apache.flink.orc.writer;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.data.Record;
import org.apache.flink.orc.util.OrcBulkWriterTestUtil;
import org.apache.flink.orc.vector.RecordVectorizer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.util.FiniteTestSource;
import org.apache.flink.util.TestLogger;

import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Integration test for writing data in ORC bulk format using StreamingFileSink.
 */
public class OrcBulkWriterITCase extends TestLogger {

	@ClassRule
	public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

	private final String schema = "struct<_col0:string,_col1:int>";
	private final List<Record> testData = Arrays.asList(
		new Record("Sourav", 41), new Record("Saul", 35), new Record("Kim", 31));

	@Test
	public void testOrcBulkWriter() throws Exception {
		final File outDir = TEMPORARY_FOLDER.newFolder();
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final Properties writerProps = new Properties();
		writerProps.setProperty("orc.compress", "LZ4");

		final OrcBulkWriterFactory<Record> factory = new OrcBulkWriterFactory<>(
			new RecordVectorizer(schema), writerProps, new Configuration());

		env.setParallelism(1);
		env.enableCheckpointing(100);

		DataStream<Record> stream = env.addSource(new FiniteTestSource<>(testData), TypeInformation.of(Record.class));
		stream.map(str -> str)
			.addSink(StreamingFileSink
				.forBulkFormat(new Path(outDir.toURI()), factory)
				.build());

		env.execute();

		OrcBulkWriterTestUtil.validate(outDir, testData);
	}
}
