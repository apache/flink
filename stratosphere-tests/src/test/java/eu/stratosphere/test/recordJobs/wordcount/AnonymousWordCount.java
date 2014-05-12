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

import java.util.Iterator;
import java.util.StringTokenizer;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

/**
 * Implements a word count which takes the input file and counts the number of
 * the occurrences of each word in the file.
 */
public class AnonymousWordCount implements Program {

	private static final long serialVersionUID = 1L;

	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int defaultParallelism = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		String inputPath = (args.length > 1 ? args[1] : "");
		String outputPath = (args.length > 2 ? args[2] : "");

		FileDataSource source = new FileDataSource(new TextInputFormat(), inputPath);

		MapOperator mapper = MapOperator.builder(new MapFunction() {
			
			private static final long serialVersionUID = 1L;

			public void map(Record record, Collector<Record> collector) throws Exception {
				String line = record.getField(0, StringValue.class).getValue();

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
		}).input(source).build();

		ReduceOperator reducer = ReduceOperator.builder(new ReduceFunction() {

			private static final long serialVersionUID = 1L;

			public void reduce(Iterator<Record> records, Collector<Record> collector) {
				Record element = null;
				int sum = 0;

				while (records.hasNext()) {
					element = records.next();
					int cnt = element.getField(1, IntValue.class).getValue();
					sum += cnt;
				}

				element.setField(1, new IntValue(sum));
				collector.collect(element);
			}
		}).keyField(StringValue.class, 0).input(mapper).build();

		FileDataSink out = new FileDataSink(new CsvOutputFormat(), outputPath, reducer, "Word Counts");
		CsvOutputFormat.configureRecordFormat(out)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(StringValue.class, 0)
			.field(IntValue.class, 1);
		
		Plan plan = new Plan(out, "WordCount Example", defaultParallelism);
		return plan;
	}
}
