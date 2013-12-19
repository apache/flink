/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.arraymodel.example;

import java.util.Iterator;

import eu.stratosphere.api.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.operators.base.MapOperatorBase;
import eu.stratosphere.api.operators.base.ReduceOperatorBase;
import eu.stratosphere.api.operators.base.ReduceOperatorBase.Combinable;
import eu.stratosphere.api.Plan;
import eu.stratosphere.api.Program;
import eu.stratosphere.api.ProgramDescription;
import eu.stratosphere.api.record.io.TextInputFormat;
import eu.stratosphere.arraymodel.ArrayModelJob;
import eu.stratosphere.arraymodel.functions.DataTypes;
import eu.stratosphere.arraymodel.functions.MapFunction;
import eu.stratosphere.arraymodel.functions.ReduceFunction;
import eu.stratosphere.arraymodel.io.StringInputFormat;
import eu.stratosphere.arraymodel.io.StringIntOutputFormat;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.AsciiUtils;
import eu.stratosphere.util.Collector;

/**
 * Implements a word count which takes the input file and counts the number of
 * the occurrences of each word in the file.
 */
public class WordCountArrayTuples implements Program, ProgramDescription {
	
	/**
	 * Converts a Record containing one string in to multiple string/integer pairs.
	 * The string is tokenized by whitespaces. For each token a new record is emitted,
	 * where the token is the first field and an Integer(1) is the second field.
	 */
	
	public static class TokenizeLine extends MapFunction {
		// initialize reusable mutable objects
		private final StringValue word = new StringValue();
		private final IntValue one = new IntValue(1);
		private final Value[] outputRecord = new Value[] { this.word, this.one };
		
		private final AsciiUtils.WhitespaceTokenizer tokenizer = 
						new AsciiUtils.WhitespaceTokenizer();
		
		@Override
		@DataTypes({StringValue.class})
		public void map(Value[] record, Collector<Value[]> collector) {
			// get the first field (as type StringValue) from the record
			StringValue line = (StringValue) record[0];
			
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
	public static class CountWords extends ReduceFunction {
		
		private final IntValue cnt = new IntValue();
		private final Value[] result = new Value[] { null, cnt };
		
		@Override
		@DataTypes({StringValue.class, IntValue.class})
		public void reduce(Iterator<Value[]> records, Collector<Value[]> out) throws Exception {
			Value[] element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				sum += ((IntValue) element[1]).getValue();
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
		
		MapOperatorBase<TokenizeLine> mapper = new MapOperatorBase<TokenizeLine>(TokenizeLine.class, "Tokenize Lines");
		mapper.setInput(source);
		
		ReduceOperatorBase<CountWords> reducer = new ReduceOperatorBase<CountWords>(CountWords.class, new int[] {0}, "Count Words");
		reducer.setInput(mapper);
		
		FileDataSink out = new FileDataSink(new StringIntOutputFormat(), output, reducer, "Word Counts");
		StringIntOutputFormat.configureArrayFormat(out)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.lenient(true);
		
		ArrayModelJob plan = new ArrayModelJob(out, "WordCount Example");
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
