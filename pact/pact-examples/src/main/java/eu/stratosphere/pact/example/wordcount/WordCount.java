/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

/**
 * 
 */
package eu.stratosphere.pact.example.wordcount;

import java.util.Iterator;
import java.util.StringTokenizer;

import eu.stratosphere.pact.common.contract.DataSinkContract;
import eu.stratosphere.pact.common.contract.DataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.io.TextOutputFormat;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.pact.common.stub.MapStub;
import eu.stratosphere.pact.common.stub.ReduceStub;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Implements a word count which takes the input file and counts the number of
 * the occurrences of each word in the file.
 * 
 * @author Larysa, Moritz Kaufmann
 */
public class WordCount implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * {@inheritDoc}
	 */
	public static class LineInFormat extends TextInputFormat<PactString, PactInteger> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean readLine(KeyValuePair<PactString, PactInteger> pair, byte[] line) {
			pair.setKey(new PactString(new String(line)));
			pair.setValue(new PactInteger(0));
			return true;
		}

	}

	/**
	 * {@inheritDoc}
	 */
	public static class WordCountOutFormat extends TextOutputFormat<PactString, PactInteger> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public byte[] writeLine(KeyValuePair<PactString, PactInteger> pair) {
			String key = pair.getKey().toString();
			String value = pair.getValue().toString();
			String line = key + " " + value + "\n";
			return line.getBytes();
		}

	}

	/**
	 * {@inheritDoc}
	 */
	public static class TokenizeLine extends MapStub<PactString, PactInteger, PactString, PactInteger> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		protected void map(PactString key, PactInteger value, Collector<PactString, PactInteger> out) {

			StringTokenizer tokenizer = new StringTokenizer(key.toString());
			while (tokenizer.hasMoreElements()) {
				String element = (String) tokenizer.nextElement();
				out.collect(new PactString(element), new PactInteger(1));
			}
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Combinable
	public static class CountWords extends ReduceStub<PactString, PactInteger, PactString, PactInteger> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void reduce(PactString key, Iterator<PactInteger> values, Collector<PactString, PactInteger> out) {
			int sum = 0;
			while (values.hasNext()) {
				PactInteger element = (PactInteger) values.next();
				sum += element.getValue();
			}

			out.collect(key, new PactInteger(sum));
		}

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void combine(PactString key, Iterator<PactInteger> values, Collector<PactString, PactInteger> out) {

			this.reduce(key, values, out);
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Plan getPlan(String... args) {
		if (args == null) {
			args = new String[0];
		}

		int noSubTasks = (args.length > 0 && args[0] != null ? Integer.parseInt(args[0]) : 3);
		String dataInput = (args.length > 1 && args[1] != null ? args[1] : "hdfs://localhost:9000/countwords/data");
		String output = (args.length > 2 && args[2] != null ? args[2] : "hdfs://localhost:9000/countwords/result");

		DataSourceContract<PactString, PactInteger> data = new DataSourceContract<PactString, PactInteger>(LineInFormat.class,
			dataInput, "Lines");
		data.setDegreeOfParallelism(noSubTasks);

		MapContract<PactString, PactInteger, PactString, PactInteger> mapper = new MapContract<PactString, PactInteger, PactString, PactInteger>(
			TokenizeLine.class, "Tokenize Lines");
		mapper.setDegreeOfParallelism(noSubTasks);
		mapper.setOutputContract(SameKey.class);

		ReduceContract<PactString, PactInteger, PactString, PactInteger> reducer = new ReduceContract<PactString, PactInteger, PactString, PactInteger>(
			CountWords.class, "Count Words");
		reducer.setDegreeOfParallelism(noSubTasks);

		DataSinkContract<PactString, PactInteger> out = new DataSinkContract<PactString, PactInteger>(WordCountOutFormat.class,
			output, "Output");
		out.setDegreeOfParallelism(noSubTasks);

		out.setInput(reducer);
		reducer.setInput(mapper);
		mapper.setInput(data);

		return new Plan(out, "WordCount Example");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "WordCount: [noSubStasks] [input] [output] <br />"
			+ "\t noSubTasks: defines the degree of parallelism <br />" + "\t input: Location of the input file <br />"
			+ "\t output: Location of the output file <br />";
	}

}
