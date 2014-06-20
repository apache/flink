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
package eu.stratosphere.hadoopcompatibility.mapreduce.example;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.hadoopcompatibility.mapreduce.HadoopInputFormat;
import eu.stratosphere.hadoopcompatibility.mapreduce.HadoopOutputFormat;
import eu.stratosphere.util.Collector;

/**
 * Implements a word count which takes the input file and counts the number of
 * occurrences of each word in the file and writes the result back to disk.
 * 
 * This example shows how to use Hadoop Input Formats, how to convert Hadoop Writables to 
 * common Java types for better usage in a Stratosphere job and how to use Hadoop Output Formats.
 */
@SuppressWarnings("serial")
public class WordCount {
	
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: WordCount <input path> <result path>");
			return;
		}
		
		final String inputPath = args[0];
		final String outputPath = args[1];
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);
		
		// Set up the Hadoop Input Format
		Job job = Job.getInstance();
		HadoopInputFormat<LongWritable, Text> hadoopInputFormat = new HadoopInputFormat<LongWritable, Text>(new TextInputFormat(), LongWritable.class, Text.class, job);
		TextInputFormat.addInputPath(job, new Path(inputPath));
		
		// Create a Stratosphere job with it
		DataSet<Tuple2<LongWritable, Text>> text = env.createInput(hadoopInputFormat);
		
		// Tokenize the line and convert from Writable "Text" to String for better handling
		DataSet<Tuple2<String, Integer>> words = text.flatMap(new Tokenizer());
		
		// Sum up the words
		DataSet<Tuple2<String, Integer>> result = words.groupBy(0).aggregate(Aggregations.SUM, 1);
		
		// Convert String back to Writable "Text" for use with Hadoop Output Format
		DataSet<Tuple2<Text, IntWritable>> hadoopResult = result.map(new HadoopDatatypeMapper());
		
		// Set up Hadoop Output Format
		HadoopOutputFormat<Text, IntWritable> hadoopOutputFormat = new HadoopOutputFormat<Text, IntWritable>(new TextOutputFormat<Text, IntWritable>(), job);
		hadoopOutputFormat.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
		hadoopOutputFormat.getConfiguration().set("mapred.textoutputformat.separator", " "); // set the value for both, since this test
		// is being executed with both types (hadoop1 and hadoop2 profile)
		TextOutputFormat.setOutputPath(job, new Path(outputPath));
		
		// Output & Execute
		hadoopResult.output(hadoopOutputFormat);
		env.execute("Word Count");
	}
	
	/**
	 * Splits a line into words and converts Hadoop Writables into normal Java data types.
	 */
	public static final class Tokenizer extends FlatMapFunction<Tuple2<LongWritable, Text>, Tuple2<String, Integer>> {
		
		@Override
		public void flatMap(Tuple2<LongWritable, Text> value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String line = value.f1.toString();
			String[] tokens = line.toLowerCase().split("\\W+");
			
			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
	
	/**
	 * Converts Java data types to Hadoop Writables.
	 */
	public static final class HadoopDatatypeMapper extends MapFunction<Tuple2<String, Integer>, Tuple2<Text, IntWritable>> {
		
		@Override
		public Tuple2<Text, IntWritable> map(Tuple2<String, Integer> value) throws Exception {
			return new Tuple2<Text, IntWritable>(new Text(value.f0), new IntWritable(value.f1));
		}
		
	}
	
}
