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

package eu.stratosphere.example.java.record.wordcount;

import java.util.Iterator;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.SimpleStringUtils;

/**
 * Implements a word count which takes the input file and counts the number of
 * the occurrences of each word in the file. Compared to the {@link WordCount} example.
 * this program performs better by using mutable objects and optimized tools.
 */
public class WordCountOptimized implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;

	/**
	 * Converts a Record containing one string in to multiple string/integer pairs.
	 * The string is tokenized by whitespaces. For each token a new record is emitted,
	 * where the token is the first field and an Integer(1) is the second field.
	 */
	public static class TokenizeLine extends MapFunction {
		
		private static final long serialVersionUID = 1L;
		
		// initialize reusable mutable objects
		private final Record outputRecord = new Record();
		private StringValue word = new StringValue();
		private final IntValue one = new IntValue(1);
		
		private final SimpleStringUtils.WhitespaceTokenizer tokenizer =
				new SimpleStringUtils.WhitespaceTokenizer();
		
		@Override
		public void map(Record record, Collector<Record> collector) {
			// get the first field (as type StringValue) from the record
			StringValue line = record.getField(0, StringValue.class);
			
			// normalize the line
			SimpleStringUtils.replaceNonWordChars(line, ' ');
			SimpleStringUtils.toLowerCase(line);
			
			// tokenize the line
			this.tokenizer.setStringToTokenize(line);
			while (tokenizer.next(this.word)) {
				// we emit a (word, 1) pair 
				this.outputRecord.setField(0, this.word);
				this.outputRecord.setField(1, this.one);
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
		
		private static final long serialVersionUID = 1L;
		
		private final IntValue cnt = new IntValue();
		
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			Record element = null;
			int sum = 0;
			while (records.hasNext()) {
				element = records.next();
				IntValue i = element.getField(1, IntValue.class);
				sum += i.getValue();
			}

			this.cnt.setValue(sum);
			element.setField(1, this.cnt);
			out.collect(element);
		}
		
		@Override
		public void combine(Iterator<Record> records, Collector<Record> out) throws Exception {
			// the logic is the same as in the reduce function, so simply call the reduce method
			this.reduce(records, out);
		}
	}


	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");

		FileDataSource source = new FileDataSource(TextInputFormat.class, dataInput, "Input Lines");
		source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");		// comment out this line for UTF-8 inputs
		MapOperator mapper = MapOperator.builder(TokenizeLine.class)
			.input(source)
			.name("Tokenize Lines")
			.build();
		ReduceOperator reducer = ReduceOperator.builder(CountWords.class, StringValue.class, 0)
			.input(mapper)
			.name("Count Words")
			.build();
		FileDataSink out = new FileDataSink(CsvOutputFormat.class, output, reducer, "Word Counts");
		CsvOutputFormat.configureRecordFormat(out)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(StringValue.class, 0)
			.field(IntValue.class, 1);
		
		Plan plan = new Plan(out, "WordCount Example");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}


	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [input] [output]";
	}
	
	// This can be used to locally run a plan from within eclipse (or anywhere else)
	public static void main(String[] args) throws Exception {
		WordCountOptimized wc = new WordCountOptimized();
		Plan plan = wc.getPlan("1", "file:///path/to/input", "file:///path/to/output");
		LocalExecutor.execute(plan);
	}
}
