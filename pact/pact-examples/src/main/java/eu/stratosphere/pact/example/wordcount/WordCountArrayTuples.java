/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.pact.array.io.StringInputFormat;
import eu.stratosphere.pact.array.io.StringIntOutputFormat;
import eu.stratosphere.pact.array.plan.Plan;
import eu.stratosphere.pact.array.stubs.DataTypes;
import eu.stratosphere.pact.array.stubs.MapStub;
import eu.stratosphere.pact.array.stubs.ReduceStub;
import eu.stratosphere.pact.array.util.AsciiUtils;
import eu.stratosphere.pact.client.LocalExecutor;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.io.TextInputFormat;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.type.base.PactString;
import eu.stratosphere.pact.generic.contract.GenericMapContract;
import eu.stratosphere.pact.generic.contract.GenericReduceContract;
import eu.stratosphere.pact.generic.contract.GenericReduceContract.Combinable;

/**
 * Implements a word count which takes the input file and counts the number of
 * the occurrences of each word in the file.
 */
public class WordCountArrayTuples implements PlanAssembler, PlanAssemblerDescription {
	
	/**
	 * Converts a PactRecord containing one string in to multiple string/integer pairs.
	 * The string is tokenized by whitespaces. For each token a new record is emitted,
	 * where the token is the first field and an Integer(1) is the second field.
	 */
	
	public static class TokenizeLine extends MapStub {
		// initialize reusable mutable objects
		private final PactString word = new PactString();
		private final PactInteger one = new PactInteger(1);
		private final Value[] outputRecord = new Value[] { this.word, this.one };
		
		private final AsciiUtils.WhitespaceTokenizer tokenizer = 
						new AsciiUtils.WhitespaceTokenizer();
		
		@Override
		@DataTypes({PactString.class})
		public void map(Value[] record, Collector<Value[]> collector) {
			// get the first field (as type PactString) from the record
			PactString line = (PactString) record[0];
			
			// normalize the line
			AsciiUtils.replaceNonWordChars(line, ' ');
			AsciiUtils.toLowerCase(line);
			
			// tokenize the line
			this.tokenizer.setStringToTokenize(line);
			while (tokenizer.next(this.word)) {
				// we emit a (word, 1) pair 
				collector.collect(this.outputRecord);
			}
		}
	}

	/**
	 * Sums up the counts for a certain given key. The counts are assumed to be at position <code>1</code>
	 * in the record. The other fields are not modified.
	 */
	@Combinable
	@ConstantFields(0)
	public static class CountWords extends ReduceStub {
		
		private final PactInteger cnt = new PactInteger();
		private final Value[] result = new Value[] { null, cnt };
		
		@Override
		@DataTypes({PactString.class, PactInteger.class})
		public void reduce(Iterator<Value[]> records, Collector<Value[]> out) throws Exception {
			Value[] element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				sum += ((PactInteger) element[1]).getValue();
			}

			this.cnt.setValue(sum);
			this.result[0] = element[0];
			out.collect(this.result);
		}
	}

	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");

		FileDataSource source = new FileDataSource(new StringInputFormat(), dataInput, "Input Lines");
		source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");		// comment out this line for UTF-8 inputs
		
		GenericMapContract<TokenizeLine> mapper = new GenericMapContract<TokenizeLine>(TokenizeLine.class, "Tokenize Lines");
		mapper.setInput(source);
		
		GenericReduceContract<CountWords> reducer = new GenericReduceContract<CountWords>(CountWords.class, new int[] {0}, "Count Words");
		reducer.setInput(mapper);
		
		FileDataSink out = new FileDataSink(new StringIntOutputFormat(), output, reducer, "Word Counts");
		StringIntOutputFormat.configureArrayFormat(out)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.lenient(true);
		
		Plan plan = new Plan(out, "WordCount Example");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [input] [output]";
	}
	
	
	public static void main(String[] args) throws Exception {
		WordCountArrayTuples wc = new WordCountArrayTuples();
		
		if (args.length < 3) {
			System.err.println(wc.getDescription());
			System.exit(1);
		}
		
		Plan plan = wc.getPlan(args);
		
		// This will execute the word-count embedded in a local context. replace this line by the commented
		// succeeding line to send the job to a local installation or to a cluster for execution
		LocalExecutor.execute(plan);
//		PlanExecutor ex = new RemoteExecutor("localhost", 6123, "target/pact-examples-0.4-SNAPSHOT-WordCount.jar");
//		ex.executePlan(plan);
	}
}
