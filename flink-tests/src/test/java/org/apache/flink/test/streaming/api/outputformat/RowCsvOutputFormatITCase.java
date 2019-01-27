/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.streaming.api.outputformat;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.io.CsvOutputFormat;
import org.apache.flink.api.java.io.RowCsvOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.testfunctions.Tokenizer;
import org.apache.flink.types.Row;

/**
 * Integration tests for {@link org.apache.flink.api.java.io.RowCsvOutputFormat}.
 */
public class RowCsvOutputFormatITCase extends StreamingProgramTestBase {

	protected String resultPath;

	@Override
	protected void preSubmit() throws Exception {
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void testProgram() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> text = env.fromElements(WordCountData.TEXT);

		DataStream<Row> counts = text
			.flatMap(new Tokenizer())
			.keyBy(0).sum(1).map(new MapFunction<Tuple2<String, Integer>, Row>() {
				@Override
				public Row map(Tuple2<String, Integer> value) throws Exception {
					return Row.of(value.getField(0), value.getField(1));
				}
			});

		counts.writeUsingOutputFormat(new RowCsvOutputFormat(new Path(resultPath),
			CsvOutputFormat.DEFAULT_LINE_DELIMITER, CsvOutputFormat.DEFAULT_FIELD_DELIMITER));

		env.execute("WriteAsCsvTest");
	}

	@Override
	protected void postSubmit() throws Exception {
		//Strip the parentheses from the expected text like output
		compareResultsByLinesInMemory(WordCountData.STREAMING_COUNTS_AS_TUPLES
			.replaceAll("[\\\\(\\\\)]", ""), resultPath);
	}
}
