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

package eu.stratosphere.test.accumulators;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.test.util.RecordAPITestBase;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import eu.stratosphere.api.common.JobExecutionResult;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.accumulators.AccumulatorHelper;
import eu.stratosphere.api.common.accumulators.DoubleCounter;
import eu.stratosphere.api.common.accumulators.Histogram;
import eu.stratosphere.api.common.accumulators.IntCounter;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.core.io.IOReadableWritable;
import eu.stratosphere.core.io.StringRecord;
import eu.stratosphere.nephele.util.SerializableHashSet;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.SimpleStringUtils;
import eu.stratosphere.util.Collector;

/**
 * Test for the basic functionality of accumulators. We cannot test all different
 * kinds of plans here (iterative, etc.).
 * 
 * TODO Test conflict when different UDFs write to accumulator with same name
 * but with different type. The conflict will occur in JobManager while merging.
 */
@RunWith(Parameterized.class)
public class AccumulatorITCase extends RecordAPITestBase {

	private static final String INPUT = "one\n" + "two two\n" + "three three three\n";
	private static final String EXPECTED = "one 1\ntwo 2\nthree 3\n";
	
	private static final int DOP = 2;

	protected String dataPath;
	protected String resultPath;
	
	public AccumulatorITCase(Configuration config) {
		super(config);
		setTaskManagerNumSlots(DOP);
	}

	@Override
	protected void preSubmit() throws Exception {
		dataPath = createTempFile("datapoints.txt", INPUT);
		resultPath = getTempFilePath("result");
	}
	
	@Override
	protected void postSubmit() throws Exception {
		compareResultsByLinesInMemory(EXPECTED, resultPath);
		
		// Test accumulator results
		System.out.println("Accumulator results:");
		JobExecutionResult res = getJobExecutionResult();
		System.out.println(AccumulatorHelper.getResultsFormated(res.getAllAccumulatorResults()));
		
		Assert.assertEquals(new Integer(3), (Integer) res.getAccumulatorResult("num-lines"));

		Assert.assertEquals(new Double(DOP), (Double)res.getAccumulatorResult("open-close-counter"));
		
		// Test histogram (words per line distribution)
		Map<Integer, Integer> dist = Maps.newHashMap();
		dist.put(1, 1); dist.put(2, 2); dist.put(3, 3);
		Assert.assertEquals(dist, res.getAccumulatorResult("words-per-line"));
		
		// Test distinct words (custom accumulator)
		Set<StringRecord> distinctWords = Sets.newHashSet();
		distinctWords.add(new StringRecord("one"));
		distinctWords.add(new StringRecord("two"));
		distinctWords.add(new StringRecord("three"));
		Assert.assertEquals(distinctWords, res.getAccumulatorResult("distinct-words"));
	}

	@Override
	protected Plan getTestJob() {
		Plan plan = getTestPlanPlan(config.getInteger("IterationAllReducer#NoSubtasks", 1), dataPath, resultPath);
		return plan;
	}

	@Parameters
	public static Collection<Object[]> getConfigurations() {
		Configuration config1 = new Configuration();
		config1.setInteger("IterationAllReducer#NoSubtasks", DOP);
		return toParameterList(config1);
	}
	
	static Plan getTestPlanPlan(int numSubTasks, String input, String output) {
		
		FileDataSource source = new FileDataSource(new TextInputFormat(), input, "Input Lines");
		source.setParameter(TextInputFormat.CHARSET_NAME, "ASCII");
		MapOperator mapper = MapOperator.builder(new TokenizeLine())
			.input(source)
			.name("Tokenize Lines")
			.build();
		ReduceOperator reducer = ReduceOperator.builder(CountWords.class, StringValue.class, 0)
			.input(mapper)
			.name("Count Words")
			.build();
		@SuppressWarnings("unchecked")
		FileDataSink out = new FileDataSink(new CsvOutputFormat("\n"," ", StringValue.class, IntValue.class), output, reducer, "Word Counts");

		Plan plan = new Plan(out, "WordCount Example");
		plan.setDefaultParallelism(numSubTasks);
		
		return plan;
	}
	
	public static class TokenizeLine extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		private final Record outputRecord = new Record();
		private StringValue word;
		private final IntValue one = new IntValue(1);
		private final SimpleStringUtils.WhitespaceTokenizer tokenizer = new SimpleStringUtils.WhitespaceTokenizer();

		// Needs to be instantiated later since the runtime context is not yet
		// initialized at this place
		IntCounter cntNumLines = null;
		Histogram wordsPerLineDistribution = null;

		// This counter will be added without convenience functions
		DoubleCounter openCloseCounter = new DoubleCounter();
		private SetAccumulator<StringRecord> distinctWords = null;
    
		@Override
		public void open(Configuration parameters) throws Exception {
		  
			// Add counters using convenience functions
			this.cntNumLines = getRuntimeContext().getIntCounter("num-lines");
			this.wordsPerLineDistribution = getRuntimeContext().getHistogram("words-per-line");

			// Add built-in accumulator without convenience function
			getRuntimeContext().addAccumulator("open-close-counter", this.openCloseCounter);

			// Add custom counter. Didn't find a way to do this with
			// getAccumulator()
			this.distinctWords = new SetAccumulator<StringRecord>();
			this.getRuntimeContext().addAccumulator("distinct-words", distinctWords);

			// Create counter and test increment
			IntCounter simpleCounter = getRuntimeContext().getIntCounter("simple-counter");
			simpleCounter.add(1);
			Assert.assertEquals(simpleCounter.getLocalValue().intValue(), 1);

			// Test if we get the same counter
			IntCounter simpleCounter2 = getRuntimeContext().getIntCounter("simple-counter");
			Assert.assertEquals(simpleCounter.getLocalValue(), simpleCounter2.getLocalValue());

			// Should fail if we request it with different type
			try {
				@SuppressWarnings("unused")
				DoubleCounter simpleCounter3 = getRuntimeContext().getDoubleCounter("simple-counter");
				// DoubleSumAggregator longAggregator3 = (DoubleSumAggregator)
				// getRuntimeContext().getAggregator("custom",
				// DoubleSumAggregator.class);
				Assert.fail("Should not be able to obtain previously created counter with different type");
			} catch (UnsupportedOperationException ex) {
			}

			// Test counter used in open() and closed()
			this.openCloseCounter.add(0.5);
		}
		
		@Override
		public void map(Record record, Collector<Record> collector) {
			this.cntNumLines.add(1);
			
			StringValue line = record.getField(0, StringValue.class);
			SimpleStringUtils.replaceNonWordChars(line, ' ');
			SimpleStringUtils.toLowerCase(line);
			this.tokenizer.setStringToTokenize(line);
			int wordsPerLine = 0;
			this.word = new StringValue();
			while (tokenizer.next(this.word))
			{
				// Use custom counter
				distinctWords.add(new StringRecord(this.word.getValue()));
  
				this.outputRecord.setField(0, this.word);
				this.outputRecord.setField(1, this.one);
				collector.collect(this.outputRecord);
				++ wordsPerLine;
			}
			wordsPerLineDistribution.add(wordsPerLine);
		}
		
		@Override
		public void close() throws Exception {
			// Test counter used in open and close only
			this.openCloseCounter.add(0.5);
			Assert.assertEquals(1, this.openCloseCounter.getLocalValue().intValue());
		}
	}

	@Combinable
	@ConstantFields(0)
	public static class CountWords extends ReduceFunction implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		private final IntValue cnt = new IntValue();
		
		private IntCounter reduceCalls = null;
		private IntCounter combineCalls = null;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			this.reduceCalls = getRuntimeContext().getIntCounter("reduce-calls");
			this.combineCalls = getRuntimeContext().getIntCounter("combine-calls");
		}
		
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			reduceCalls.add(1);
			reduceInternal(records, out);
		}
		
		@Override
		public void combine(Iterator<Record> records, Collector<Record> out) throws Exception {
			combineCalls.add(1);
			reduceInternal(records, out);
		}
		
		private void reduceInternal(Iterator<Record> records, Collector<Record> out) {
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
	
	/**
	 * Custom accumulator
	 */
	public static class SetAccumulator<T extends IOReadableWritable> implements Accumulator<T, Set<T>> {

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
		public void write(DataOutput out) throws IOException {
			this.set.write(out);
		}

		@Override
		public void read(DataInput in) throws IOException {
			this.set.read(in);
		}
	}
}
