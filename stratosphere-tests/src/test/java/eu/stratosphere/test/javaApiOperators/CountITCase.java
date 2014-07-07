/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.test.javaApiOperators;

import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.io.LocalCollectionOutputFormat;
import eu.stratosphere.test.util.JavaProgramTestBase;
import eu.stratosphere.util.Collector;
import org.eclipse.jetty.util.ArrayQueue;

import java.util.Queue;

import static org.junit.Assert.assertEquals;

public class CountITCase extends JavaProgramTestBase {

	@Override
	protected void testProgram() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		Queue<Long> countResults = new ArrayQueue<Long>();
		OutputFormat<Long> localOutputFormat = new LocalCollectionOutputFormat<Long>(countResults);

		DataSet<String> text = env.fromElements(
				"Who's there?",
				"I think I hear them. Stand, ho! Who's there?");

		// 2 elements
		text.count().output(localOutputFormat);

		// 11 elements
		text.flatMap(new LineSplitter()).count().output(localOutputFormat);

		// 0 elements
		text.filter(new FilterAll()).count().output(localOutputFormat);

		env.execute();

		assertEquals(2, countResults.remove().longValue());
		assertEquals(11, countResults.remove().longValue());
		assertEquals(0, countResults.remove().longValue());
	}

	private static class LineSplitter extends FlatMapFunction<String, String> {
		@Override
		public void flatMap(String value, Collector<String> out) throws Exception {
			for (String word : value.split(" ")) {
				out.collect(word);
			}
		}
	}

	private static class FilterAll extends FilterFunction<String> {
		@Override
		public boolean filter(String value) throws Exception {
			return false;
		}
	}
}
