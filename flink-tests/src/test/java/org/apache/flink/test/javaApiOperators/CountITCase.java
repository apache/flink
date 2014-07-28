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

package org.apache.flink.test.javaApiOperators;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FilterFunction;
import org.apache.flink.api.java.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.javaApiOperators.util.CollectionDataSets;
import org.apache.flink.test.util.JavaProgramTestBase;
import org.apache.flink.util.Collector;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CountITCase extends JavaProgramTestBase {

	@Override
	protected void testProgram() throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		List<Long> countResults = new ArrayList<Long>();
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

		Assert.assertEquals(2, countResults.remove(0).longValue());
		Assert.assertEquals(11, countResults.remove(0).longValue());
		Assert.assertEquals(0, countResults.remove(0).longValue());

		// --------------------------------------------------------------------

		DataSet<Tuple3<Integer, Long, String>> tuples = CollectionDataSets.get3TupleDataSet(env);

		// 21 * 1
		tuples.groupBy(0).count().output(localOutputFormat);

		env.execute();

		for (int i = 0; i < 21; i++) {
			Assert.assertEquals(1, countResults.remove(0).longValue());
		}

		// 1, 2, 3, 4, 5, 6
		tuples.groupBy(1).count().output(localOutputFormat);

		env.execute();

		Collections.sort(countResults);

		Assert.assertEquals(1, countResults.remove(0).longValue());
		Assert.assertEquals(2, countResults.remove(0).longValue());
		Assert.assertEquals(3, countResults.remove(0).longValue());
		Assert.assertEquals(4, countResults.remove(0).longValue());
		Assert.assertEquals(5, countResults.remove(0).longValue());
		Assert.assertEquals(6, countResults.remove(0).longValue());

		// --------------------------------------------------------------------

		DataSet<CollectionDataSets.CustomType> custom = CollectionDataSets.getCustomTypeDataSet(env);

		custom.groupBy("myLong").count().output(localOutputFormat);

		env.execute();

		for (int i = 0; i < 21; i++) {
			Assert.assertEquals(1, countResults.remove(0).longValue());
		}

		custom.groupBy("myInt").count().output(localOutputFormat);

		env.execute();

		Collections.sort(countResults);

		Assert.assertEquals(1, countResults.remove(0).longValue());
		Assert.assertEquals(2, countResults.remove(0).longValue());
		Assert.assertEquals(3, countResults.remove(0).longValue());
		Assert.assertEquals(4, countResults.remove(0).longValue());
		Assert.assertEquals(5, countResults.remove(0).longValue());
		Assert.assertEquals(6, countResults.remove(0).longValue());

		// --------------------------------------------------------------------

		custom.groupBy(new CustomMyLongKeySelector()).count().output(localOutputFormat);

		env.execute();

		for (int i = 0; i < 21; i++) {
			Assert.assertEquals(1, countResults.remove(0).longValue());
		}

		custom.groupBy(new CustomMyIntKeySelector()).count().output(localOutputFormat);

		env.execute();

		Collections.sort(countResults);

		Assert.assertEquals(1, countResults.remove(0).longValue());
		Assert.assertEquals(2, countResults.remove(0).longValue());
		Assert.assertEquals(3, countResults.remove(0).longValue());
		Assert.assertEquals(4, countResults.remove(0).longValue());
		Assert.assertEquals(5, countResults.remove(0).longValue());
		Assert.assertEquals(6, countResults.remove(0).longValue());
	}

	// ------------------------------------------------------------------------

	private static class LineSplitter extends FlatMapFunction<String, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<String> out) throws Exception {
			for (String word : value.split(" ")) {
				out.collect(word);
			}
		}
	}

	private static class FilterAll extends FilterFunction<String> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(String value) throws Exception {
			return false;
		}
	}

	private static class CustomMyLongKeySelector extends KeySelector<CollectionDataSets.CustomType, Long> {

		private static final long serialVersionUID = 1L;

		@Override
		public Long getKey(CollectionDataSets.CustomType in) {
			return in.myLong;
		}
	}

	private static class CustomMyIntKeySelector extends KeySelector<CollectionDataSets.CustomType, Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public Integer getKey(CollectionDataSets.CustomType in) {
			return in.myInt;
		}
	}
}
