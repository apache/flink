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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.testfunctions.Tokenizer;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

/**
 * Integration tests for {@link org.apache.flink.api.java.io.CsvOutputFormat}.
 */
public class CsvOutputFormatITCase extends AbstractTestBase {

	@Test
	public void testProgram() throws Exception {
		String resultPath = getTempDirPath("result");

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> text = env.fromElements(WordCountData.TEXT);

		DataStream<Tuple2<String, Integer>> counts = text
				.flatMap(new Tokenizer())
				.keyBy(0).sum(1);

		counts.writeAsCsv(resultPath);

		env.execute("WriteAsCsvTest");

		//Strip the parentheses from the expected text like output
		compareResultsByLinesInMemory(WordCountData.STREAMING_COUNTS_AS_TUPLES
				.replaceAll("[\\\\(\\\\)]", ""), resultPath);
	}

}

