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

package eu.stratosphere.pact.example.wordcount;

import java.util.Iterator;
import java.util.StringTokenizer;

import eu.stratosphere.pact.common.contract.FileDataSinkContract;
import eu.stratosphere.pact.common.contract.FileDataSourceContract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.OutputContract.SameKey;
import eu.stratosphere.pact.common.contract.ReduceContract;
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
import eu.stratosphere.pact.common.type.base.PactNull;
import eu.stratosphere.pact.common.type.base.PactString;

/**
 * Implements a word count which takes the input file and counts the number of
 * the occurrences of each word in the file.
 * 
 * @author Larysa, Moritz Kaufmann, mjsax
 */
public class WordCountDouble2 implements PlanAssembler, PlanAssemblerDescription {

	/**
	 * Converts a input string (a line) into a KeyValuePair with the string
	 * being the key and the value being a zero Integer.
	 */
	public static class LineInFormat extends TextInputFormat<PactNull, PactString> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public boolean readLine(KeyValuePair<PactNull, PactString> pair, byte[] line) {
			pair.setKey(new PactNull());
			pair.setValue(new PactString(new String(line)));
			return true;
		}

	}

	/**
	 * Writes a (String,Integer)-KeyValuePair to a string. The output format is:
	 * "&lt;key&gt;&nbsp;&lt;value&gt;\nl"
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
	 * Converts a (String,Integer)-KeyValuePair into multiple KeyValuePairs. The
	 * key string is tokenized by spaces. For each token a new
	 * (String,Integer)-KeyValuePair is emitted where the Token is the key and
	 * an Integer(1) is the value.
	 */
	public static class TokenizeLine extends MapStub<PactNull, PactString, PactString, PactInteger> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void map(PactNull key, PactString value, Collector<PactString, PactInteger> out) {

			String line = value.toString();
			line = line.replaceAll("\\W", " ");
			line = line.toLowerCase();
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreElements()) {
				String element = (String) tokenizer.nextElement();
				out.collect(new PactString(element), new PactInteger(1));
			}
		}

	}

	/**
	 * Counts the number of values for a given key. Hence, the number of
	 * occurences of a given token (word) is computed and emitted. The key is
	 * not modified, hence a SameKey OutputContract is attached to this class.
	 */
	@SameKey
	@Combinable
	public static class CountWords extends ReduceStub<PactString, PactInteger, PactString, PactInteger> {

		/**
		 * {@inheritDoc}
		 */
		@Override
		public void reduce(PactString key, Iterator<PactInteger> values, Collector<PactString, PactInteger> out) {
			int sum = 0;
			while (values.hasNext()) {
				PactInteger element = values.next();
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

		// parse job parameters
		int noSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");

		FileDataSourceContract<PactNull, PactString> data1 = new FileDataSourceContract<PactNull, PactString>(
				LineInFormat.class, dataInput, "Input Lines");
		data1.setDegreeOfParallelism(noSubTasks);

		FileDataSourceContract<PactNull, PactString> data2 = new FileDataSourceContract<PactNull, PactString>(
				LineInFormat.class, dataInput, "Input Lines");
		data2.setDegreeOfParallelism(noSubTasks);

		MapContract<PactNull, PactString, PactString, PactInteger> mapper1 = new MapContract<PactNull, PactString, PactString, PactInteger>(
				TokenizeLine.class, "Tokenize Lines");
		mapper1.setDegreeOfParallelism(noSubTasks);

		MapContract<PactNull, PactString, PactString, PactInteger> mapper2 = new MapContract<PactNull, PactString, PactString, PactInteger>(
				TokenizeLine.class, "Tokenize Lines");
		mapper2.setDegreeOfParallelism(noSubTasks);

		ReduceContract<PactString, PactInteger, PactString, PactInteger> reducer = new ReduceContract<PactString, PactInteger, PactString, PactInteger>(
				CountWords.class, "Count Words");
		reducer.setDegreeOfParallelism(noSubTasks);

		FileDataSinkContract<PactString, PactInteger> out = new FileDataSinkContract<PactString, PactInteger>(
				WordCountOutFormat.class, output, "Output");
		out.setDegreeOfParallelism(noSubTasks);

		out.addInput(reducer);
		reducer.addInput(mapper1);
		reducer.addInput(mapper2);
		mapper1.addInput(data1);
		mapper2.addInput(data2);

		return new Plan(out, "WordCount Example");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String getDescription() {
		return "Parameters: [noSubStasks] [input] [output]";
	}

}
