/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.test.exampleJavaPrograms;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.io.LocalCollectionOutputFormat;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.example.java.wordcount.WordCount;
import eu.stratosphere.test.testdata.WordCountData;
import eu.stratosphere.test.util.JavaProgramTestBase;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WordCountWithCollectionITCase extends JavaProgramTestBase {

	List<Tuple2<String, Integer>> resultsCollected = new ArrayList<Tuple2<String, Integer>>();



	@Override
	protected void postSubmit() throws Exception {
		String[] result = new String[resultsCollected.size()];
		for (int i = 0; i < result.length; i++) {
			result[i] = resultsCollected.get(i).toString();
		}
		Arrays.sort(result);

		String[] expected = WordCountData.COUNTS_AS_TUPLES.split("\n");
		Arrays.sort(expected);

		Assert.assertEquals("Different number of lines in expected and obtained result.", expected.length, result.length);
		Assert.assertArrayEquals(expected, result);
	}


	@Override
	protected void testProgram() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> text = env.fromElements(WordCountData.TEXT);
		DataSet<Tuple2<String, Integer>> words = text.flatMap(new WordCount.Tokenizer());
		DataSet<Tuple2<String, Integer>> result = words.groupBy(0).aggregate(Aggregations.SUM, 1);

		result.output(new LocalCollectionOutputFormat(resultsCollected));
		env.execute("Word Count Collection");

	}


}
