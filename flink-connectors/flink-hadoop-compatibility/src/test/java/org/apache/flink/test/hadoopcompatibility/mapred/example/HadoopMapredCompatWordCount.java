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

package org.apache.flink.test.hadoopcompatibility.mapred.example;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopInputFormat;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.hadoopcompatibility.mapred.HadoopMapFunction;
import org.apache.flink.hadoopcompatibility.mapred.HadoopReduceCombineFunction;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * Implements a word count which takes the input file and counts the number of
 * occurrences of each word in the file and writes the result back to disk.
 *
 * <p>This example shows how to use Hadoop Input Formats, how to convert Hadoop Writables to
 * common Java types for better usage in a Flink job and how to use Hadoop Output Formats.
 */
public class HadoopMapredCompatWordCount {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: WordCount <input path> <result path>");
			return;
		}

		final String inputPath = args[0];
		final String outputPath = args[1];

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// Set up the Hadoop Input Format
		HadoopInputFormat<LongWritable, Text> hadoopInputFormat = new HadoopInputFormat<LongWritable, Text>(new TextInputFormat(), LongWritable.class, Text.class, new JobConf());
		TextInputFormat.addInputPath(hadoopInputFormat.getJobConf(), new Path(inputPath));

		// Create a Flink job with it
		DataSet<Tuple2<LongWritable, Text>> text = env.createInput(hadoopInputFormat);

		DataSet<Tuple2<Text, LongWritable>> words =
				text.flatMap(new HadoopMapFunction<LongWritable, Text, Text, LongWritable>(new Tokenizer()))
					.groupBy(0).reduceGroup(new HadoopReduceCombineFunction<Text, LongWritable, Text, LongWritable>(new Counter(), new Counter()));

		// Set up Hadoop Output Format
		HadoopOutputFormat<Text, LongWritable> hadoopOutputFormat =
				new HadoopOutputFormat<Text, LongWritable>(new TextOutputFormat<Text, LongWritable>(), new JobConf());
		hadoopOutputFormat.getJobConf().set("mapred.textoutputformat.separator", " ");
		TextOutputFormat.setOutputPath(hadoopOutputFormat.getJobConf(), new Path(outputPath));

		// Output & Execute
		words.output(hadoopOutputFormat).setParallelism(1);
		env.execute("Hadoop Compat WordCount");
	}

	/**
	 * A {@link Mapper} that splits a line into words.
	 */
	public static final class Tokenizer implements Mapper<LongWritable, Text, Text, LongWritable> {

		@Override
		public void map(LongWritable k, Text v, OutputCollector<Text, LongWritable> out, Reporter rep)
				throws IOException {
			// normalize and split the line
			String line = v.toString();
			String[] tokens = line.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Text(token), new LongWritable(1L));
				}
			}
		}

		@Override
		public void configure(JobConf arg0) { }

		@Override
		public void close() throws IOException { }

	}

	/**
	 * A {@link Reducer} to sum counts.
	 */
	public static final class Counter implements Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		public void reduce(Text k, Iterator<LongWritable> vs, OutputCollector<Text, LongWritable> out, Reporter rep)
				throws IOException {

			long cnt = 0;
			while (vs.hasNext()) {
				cnt += vs.next().get();
			}
			out.collect(k, new LongWritable(cnt));

		}

		@Override
		public void configure(JobConf arg0) { }

		@Override
		public void close() throws IOException { }
	}

}
