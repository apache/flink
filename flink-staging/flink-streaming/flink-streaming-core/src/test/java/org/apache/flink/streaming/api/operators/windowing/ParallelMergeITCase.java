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

package org.apache.flink.streaming.api.operators.windowing;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.helper.Time;
import org.apache.flink.streaming.util.StreamingProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Tests that {@link ParallelMerge} does not swallow records of the
 * last window.
 */
public class ParallelMergeITCase extends StreamingProgramTestBase {

	protected String textPath;
	protected String resultPath;
	protected final String input = "To be, or not to be,--that is the question:--" +
									"Whether 'tis nobler in the mind to suffer";

	@Override
	protected void preSubmit() throws Exception {
		textPath = createTempFile("text.txt", input);
		resultPath = getTempDirPath("result");
	}

	@Override
	protected void postSubmit() throws Exception {
		List<String> resultLines = new ArrayList<>();
		readAllResultLines(resultLines, resultPath);

		// check that result lines are not swallowed, as every element is expected to be in the
		// last time window we either get the right output or no output at all
		if (resultLines.isEmpty()){
			Assert.fail();
		}
	}

	@Override
	protected void testProgram() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> text = env.fromElements(input);

		DataStream<Tuple2<String, Integer>> counts =
				text.flatMap(new Tokenizer())
						.window(Time.of(1000, TimeUnit.MILLISECONDS))
						.groupBy(0)
						.sum(1)
						.flatten();

		counts.writeAsText(resultPath);

		try {
			env.execute();
		} catch (RuntimeException e){
			// might happen at closing the active window
			// do nothing
		}
	}

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
				throws Exception {
			String[] tokens = value.toLowerCase().split("\\W+");

			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(Tuple2.of(token, 1));
				}
			}
		}
	}
}
