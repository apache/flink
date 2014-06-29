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

package eu.stratosphere.test.recordJobs.wordcount;

import java.io.IOException;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.accumulators.Histogram;
import eu.stratosphere.api.common.accumulators.LongCounter;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.nephele.util.SerializableHashSet;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.types.Value;
import eu.stratosphere.util.Collector;

/**
 * This is similar to the WordCount example and additionally demonstrates how to
 * use custom accumulators (built-in or custom).
 */
public class WordCountAccumulators implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;

	public static class TokenizeLine extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;

		// For efficiency it is recommended to have member variables for the accumulators
		public static final String ACCUM_NUM_LINES = "accumulator.num-lines";
		private LongCounter numLines = new LongCounter();

		// This histogram accumulator collects the distribution of number of words per line
		public static final String ACCUM_WORDS_PER_LINE = "accumulator.words-per-line";
		private Histogram wordsPerLine = new Histogram();

		public static final String ACCUM_DISTINCT_WORDS = "accumulator.distinct-words";
		private SetAccumulator<StringValue> distinctWords = new SetAccumulator<StringValue>();

		
		@Override
		public void open(Configuration parameters) throws Exception {

			// Accumulators have to be registered to the system
			getRuntimeContext().addAccumulator(ACCUM_NUM_LINES, this.numLines);
			getRuntimeContext().addAccumulator(ACCUM_WORDS_PER_LINE, this.wordsPerLine);
			getRuntimeContext().addAccumulator(ACCUM_DISTINCT_WORDS, this.distinctWords);

			// You could also write to accumulators in open() or close()
		}

		
		@Override
		public void map(Record record, Collector<Record> collector) {
			
			// Increment counter
			numLines.add(1L);
						
			// get the first field (as type StringValue) from the record
			String line = record.getField(0, StringValue.class).getValue();

			// normalize the line
			line = line.replaceAll("\\W+", " ").toLowerCase();
			
			// tokenize the line
			StringTokenizer tokenizer = new StringTokenizer(line);
			int numWords = 0;
			
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				
				distinctWords.add(new StringValue(word));
				++numWords;
				
				// we emit a (word, 1) pair 
				collector.collect(new Record(new StringValue(word), new IntValue(1)));
			}
			
			// Add a value to the histogram accumulator
			this.wordsPerLine.add(numWords);
		}
	}

	@Combinable
	@ConstantFields(0)
	public static class CountWords extends ReduceFunction implements Serializable {

		private static final long serialVersionUID = 1L;

		private final IntValue cnt = new IntValue();

		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) {
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
	}

	@Override
	public Plan getPlan(String... args) {
		int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output = (args.length > 2 ? args[2] : "");

		FileDataSource source = new FileDataSource(new TextInputFormat(), dataInput, "Input Lines");

		MapOperator mapper = MapOperator.builder(new TokenizeLine()).input(source).name("Tokenize Lines").build();
		
		ReduceOperator reducer = ReduceOperator.builder(CountWords.class, StringValue.class, 0).input(mapper)
				.name("Count Words").build();
		
		FileDataSink out = new FileDataSink(new CsvOutputFormat(), output, reducer, "Word Counts");
		
		CsvOutputFormat.configureRecordFormat(out).recordDelimiter('\n')
				.fieldDelimiter(' ').field(StringValue.class, 0)
				.field(IntValue.class, 1);

		Plan plan = new Plan(out, "WordCount Example");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}

	@Override
	public String getDescription() {
		return "Parameters: [numSubStasks] [input] [output]";
	}

	public static void main(String[] args) throws Exception {
		WordCountAccumulators wc = new WordCountAccumulators();

		if (args.length < 3) {
			System.err.println(wc.getDescription());
			System.exit(1);
		}

		Plan plan = wc.getPlan(args);

		JobExecutionResult result = LocalExecutor.execute(plan);

		// Accumulators can be accessed by their name. 
		System.out.println("Number of lines counter: "+ result.getAccumulatorResult(TokenizeLine.ACCUM_NUM_LINES));
		System.out.println("Words per line histogram: " + result.getAccumulatorResult(TokenizeLine.ACCUM_WORDS_PER_LINE));
		System.out.println("Distinct words: " + result.getAccumulatorResult(TokenizeLine.ACCUM_DISTINCT_WORDS));
	}

	/**
	 * Custom accumulator
	 */
	public static class SetAccumulator<T extends Value> implements Accumulator<T, Set<T>> {

		private static final long serialVersionUID = 1L;

		private SerializableHashSet<T> set = new SerializableHashSet<T>();

		@Override
		public void add(T value) {
			this.set.add(value);
		}

		@Override
		public Set<T> getLocalValue() {
			return this.set;
		}

		@Override
		public void resetLocal() {
			this.set.clear();
		}

		@Override
		public void merge(Accumulator<T, Set<T>> other) {
			// build union
			this.set.addAll(((SetAccumulator<T>) other).getLocalValue());
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			this.set.write(out);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			this.set.read(in);
		}
	}
}
