/***********************************************************************************************************************
 *
 * Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.pact.test.pactPrograms;

import java.util.Collection;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.plan.Plan;
import eu.stratosphere.api.plan.PlanAssembler;
import eu.stratosphere.api.plan.PlanAssemblerDescription;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.io.MutableUnionRecordReader;
import eu.stratosphere.nephele.io.UnionRecordReader;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.io.RecordOutputFormat;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.example.wordcount.WordCount.CountWords;
import eu.stratosphere.pact.example.wordcount.WordCount.TokenizeLine;
import eu.stratosphere.pact.test.util.TestBase2;
import eu.stratosphere.types.PactInteger;
import eu.stratosphere.types.PactString;

/**
 * WordCount with multiple inputs to the reducer.
 * <p>
 * This test case is an adaption of issue #192 (and #124), which revealed problems with the union readers in Nephele.
 * The problems have been fixed with commit 1228a5e. Without this commit the test will deadlock.
 * 
 * @see {@link https://github.com/stratosphere/stratosphere/issues/192}
 * @see {@link https://github.com/stratosphere/stratosphere/issues/124}
 * @see {@link UnionRecordReader}
 * @see {@link MutableUnionRecordReader}
 */
@RunWith(Parameterized.class)
public class WordCountUnionReduceITCase extends TestBase2 {

	private final String INPUT = WordCountITCase.TEXT;

	private final String EXPECTED_COUNTS = WordCountITCase.COUNTS;

	private String inputPath;

	private String secondInputPath;

	private String outputPath;

	public WordCountUnionReduceITCase(Configuration config) {
		super(config);
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config = new Configuration();

		// with just a single subtask the test didn't deadlock before the fix
		config.setInteger("WordCountUnionReduce#NumSubtasks", 4);

		// the fixed input is repeated this many times and the expected counts
		// are multiplied by this factor, because the problem only occurs with
		// inputs of a certain size
		config.setInteger("WordCountUnionReduce#InputSizeFactor", 6144);

		return toParameterList(config);
	}

	@Override
	protected void preSubmit() throws Exception {
		String input = repeatString(INPUT, this.config.getInteger("WordCountUnionReduce#InputSizeFactor", 1));

		this.inputPath = createTempFile("input.txt", input);
		this.outputPath = getTempDirPath("output");
	}

	@Override
	protected Plan getPactPlan() {
		WordCountUnionReduce wc = new WordCountUnionReduce();
		return wc.getPlan(this.inputPath, this.secondInputPath, this.outputPath,
			this.config.getString("WordCountUnionReduce#NumSubtasks", "1"));
	}

	@Override
	protected void postSubmit() throws Exception {
		String expectedCounts =
			multiplyIntegersInString(EXPECTED_COUNTS,
				// adjust counts to string repetition (InputSizeFactor) and two mappers (*2)
				this.config.getInteger("WordCountUnionReduce#InputSizeFactor", 1) * 2);
		compareResultsByLinesInMemory(expectedCounts, this.outputPath);
	}

	/**
	 * This is the adapted plan from issue #192.
	 * 
	 * @see {@link https://github.com/stratosphere/stratosphere/issues/192}
	 */
	private class WordCountUnionReduce implements PlanAssembler, PlanAssemblerDescription {

		@Override
		public String getDescription() {
			return "Usage: [input path] [output path] [num subtasks]";
		}

		/**
		 * <pre>
		 *                   +-------------+
		 *              //=> | MapContract | =\\
		 * +--------+  //    +-------------+   \\   +----------------+    +------+
		 * | Source | =|                        |=> | ReduceContract | => | Sink |
		 * +--------+  \\    +-------------+   //   +----------------+    +------+
		 *              \\=> | MapContract | =//
		 *                   +-------------+
		 * </pre>
		 */
		@Override
		public Plan getPlan(String... args) {
			String inputPath = args.length >= 1 ? args[0] : "";
			String outputPath = args.length >= 3 ? args[2] : "";
			int numSubtasks = args.length >= 4 ? Integer.parseInt(args[3]) : 1;

			FileDataSource source = new FileDataSource(TextInputFormat.class, inputPath, "First Input");

			MapContract wordsFirstInput = MapContract.builder(TokenizeLine.class)
				.input(source)
				.name("Words (First Input)")
				.build();

			MapContract wordsSecondInput = MapContract.builder(TokenizeLine.class)
				.input(source)
				.name("Words (Second Input)")
				.build();

			ReduceContract counts = ReduceContract.builder(CountWords.class, PactString.class, 0)
				.input(wordsFirstInput, wordsSecondInput)
				.name("Word Counts")
				.build();

			FileDataSink sink = new FileDataSink(RecordOutputFormat.class, outputPath, counts);
			RecordOutputFormat.configureRecordFormat(sink)
				.recordDelimiter('\n')
				.fieldDelimiter(' ')
				.field(PactString.class, 0)
				.field(PactInteger.class, 1);

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
