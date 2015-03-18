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

package org.apache.flink.test.recordJobTests;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.io.TextInputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.test.recordJobs.wordcount.WordCount.CountWords;
import org.apache.flink.test.recordJobs.wordcount.WordCount.TokenizeLine;
import org.apache.flink.test.testdata.WordCountData;
import org.apache.flink.test.util.RecordAPITestBase;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.StringValue;

/**
 * WordCount with multiple inputs to the reducer.
 * <p>
 * This test case is an adaption of issue #192 (and #124), which revealed problems with the union readers in Nephele.
 * The problems have been fixed with commit 1228a5e. Without this commit the test will deadlock.
 */
@SuppressWarnings("deprecation")
public class WordCountUnionReduceITCase extends RecordAPITestBase {

	private static final int MULTIPLY = 1000;
	
	private String inputPath;

	private String outputPath;

	public WordCountUnionReduceITCase(){
		setTaskManagerNumSlots(parallelism);
	}


	@Override
	protected void preSubmit() throws Exception {
		// the fixed input is repeated this many times and the expected counts
		// are multiplied by this factor, because the problem only occurs with
		// inputs of a certain size
		String input = repeatString(WordCountData.TEXT, MULTIPLY);

		this.inputPath = createTempFile("input.txt", input);
		this.outputPath = getTempDirPath("output");
	}

	@Override
	protected Plan getTestJob() {
		WordCountUnionReduce wc = new WordCountUnionReduce();
		return wc.getPlan(this.inputPath, this.outputPath, parallelism);
	}

	@Override
	protected void postSubmit() throws Exception {
		String expectedCounts =
			multiplyIntegersInString(WordCountData.COUNTS,
				// adjust counts to string repetition (InputSizeFactor) and two mappers (*2)
				MULTIPLY * 2);
		compareResultsByLinesInMemory(expectedCounts, this.outputPath);
	}

	/**
	 * This is the adapted plan from issue #192.
	 */
	private class WordCountUnionReduce {

		/**
		 * <pre>
		 *                   +-------------+
		 *              //=> | MapOperator | =\\
		 * +--------+  //    +-------------+   \\   +----------------+    +------+
		 * | Source | =|                        |=> | ReduceOperator | => | Sink |
		 * +--------+  \\    +-------------+   //   +----------------+    +------+
		 *              \\=> | MapOperator | =//
		 *                   +-------------+
		 * </pre>
		 */
		public Plan getPlan(String inputPath, String outputPath, int numSubtasks) {

			FileDataSource source = new FileDataSource(TextInputFormat.class, inputPath, "First Input");

			MapOperator wordsFirstInput = MapOperator.builder(TokenizeLine.class)
				.input(source)
				.name("Words (First Input)")
				.build();

			MapOperator wordsSecondInput = MapOperator.builder(TokenizeLine.class)
				.input(source)
				.name("Words (Second Input)")
				.build();

			@SuppressWarnings("unchecked")
			ReduceOperator counts = ReduceOperator.builder(CountWords.class, StringValue.class, 0)
				.input(wordsFirstInput, wordsSecondInput)
				.name("Word Counts")
				.build();

			FileDataSink sink = new FileDataSink(CsvOutputFormat.class, outputPath, counts);
			CsvOutputFormat.configureRecordFormat(sink)
				.recordDelimiter('\n')
				.fieldDelimiter(' ')
				.field(StringValue.class, 0)
				.field(IntValue.class, 1);

			Plan plan = new Plan(sink, "WordCount Union Reduce");
			plan.setDefaultParallelism(numSubtasks);

			return plan;
		}
	}

	/**
	 * Repeats the given String and returns the resulting String.
	 * 
	 * @param str
	 *        the string to repeat
	 * @param n
	 *        the number of times to repeat the string
	 * @return repeated string if n > 1, otherwise the input string
	 */
	private String repeatString(String str, int n) {
		if (n <= 1) {
			return str;
		}

		StringBuilder sb = new StringBuilder(str.length() * n + 1);
		for (int i = 0; i < n; i++) {
			sb.append(str);
		}
		return sb.toString();
	}

	/**
	 * Returns a new String with all occurring integers multiplied.
	 * 
	 * @param str
	 *        the string which contains integers to multiply
	 * @param n
	 *        the factor to multiply each integer with
	 * @return new string with multiplied integers
	 */
	private String multiplyIntegersInString(String str, int n) {
		Pattern counts = Pattern.compile("(\\d+)");
		Matcher matcher = counts.matcher(str);

		StringBuffer sb = new StringBuffer(str.length());

		boolean hasMatch = false;

		while (matcher.find()) {
			hasMatch = true;
			matcher.appendReplacement(sb, String.valueOf(n * Integer.parseInt(matcher.group(1))));
		}

		return hasMatch ? sb.toString() : str;
	}
}
