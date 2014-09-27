/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.hadoopcompatibility.mapred.record.example;

import java.io.Serializable;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.Program;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.record.functions.MapFunction;
import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFields;
import org.apache.flink.api.java.record.io.CsvOutputFormat;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator.Combinable;
import org.apache.flink.client.LocalExecutor;
import org.apache.flink.hadoopcompatibility.mapred.record.HadoopDataSource;
import org.apache.flink.hadoopcompatibility.mapred.record.datatypes.WritableWrapperConverter;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;

/**
 * Implements a word count which takes the input file and counts the number of
 * the occurrences of each word in the file. 
 * 
 * <br /><br />
 * 
 * <b>Note</b>: This example uses the out dated Record API.
 * It is recommended to use the new Java API.
 * 
 * @see org.apache.flink.hadoopcompatibility.mapred.example.WordCount
 */
public class WordCount implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;


	/**
	 * Converts a Record containing one string in to multiple string/integer pairs.
	 * The string is tokenized by whitespaces. For each token a new record is emitted,
	 * where the token is the first field and an Integer(1) is the second field.
	 */
	public static class TokenizeLine extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void map(Record record, Collector<Record> collector) {
			// get the first field (as type StringValue) from the record
			String line = record.getField(1, StringValue.class).getValue();
			// normalize the line
			line = line.replaceAll("\\W+", " ").toLowerCase();
			
			// tokenize the line
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				
				// we emit a (word, 1) pair 
				collector.collect(new Record(new StringValue(word), new IntValue(1)));
			}
		}
	}

	/**
	 * Sums up the counts for a certain given key. The counts are assumed to be at position <code>1</code>
	 * in the record. The other fields are not modified.
	 */
	@Combinable
	@ConstantFields(0)
	public static class CountWords extends ReduceFunction implements Serializable {
		
		private static final long serialVersionUID = 1L;
		
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
			Record element = null;
			int sum = 0;
			
			while (records.hasNext()) {
				element = records.next();
				int cnt = element.getField(1, IntValue.class).getValue();
				sum += cnt;
			}

			element.setField(1, new IntValue(sum));
			out.collect(element);
		}
		
		@Override
		public void combine(Iterator<Record> records, Collector<Record> out) throws Exception {
			// the logic is the same as in the reduce function, so simply call the reduce method
			reduce(records, out);
		}
	}


	@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks   = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String dataInput = (args.length > 1 ? args[1] : "");
		String output    = (args.length > 2 ? args[2] : "");
		
		
		HadoopDataSource source = new HadoopDataSource(new TextInputFormat(), new JobConf(), "Input Lines");
		TextInputFormat.addInputPath(source.getJobConf(), new Path(dataInput));
		
		// Example with Wrapper Converter
		HadoopDataSource<LongWritable,Text> sourceHadoopType = new HadoopDataSource<LongWritable, Text>(
				new TextInputFormat(), new JobConf(), "Input Lines", new WritableWrapperConverter<LongWritable, Text>());
		TextInputFormat.addInputPath(source.getJobConf(), new Path(dataInput));
		
		MapOperator mapper = MapOperator.builder(new TokenizeLine())
			.input(source)
			.name("Tokenize Lines")
			.build();
		ReduceOperator reducer = ReduceOperator.builder(CountWords.class, StringValue.class, 0)
			.input(mapper)
			.name("Count Words")
			.build();
		FileDataSink out = new FileDataSink(new CsvOutputFormat(), output, reducer, "Word Counts");
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

	
	public static void main(String[] args) throws Exception {
		WordCount wc = new WordCount();
		
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
