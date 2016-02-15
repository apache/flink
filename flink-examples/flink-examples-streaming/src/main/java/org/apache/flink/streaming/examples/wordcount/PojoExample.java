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

package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * This example shows an implementation of WordCount without using the Tuple2
 * type, but a custom class.
 * 
 * <p>
 * Usage: <code>WordCount --input &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 * 
 * <p>
 * This example shows how to:
 * <ul>
 * <li>use POJO data types,
 * <li>write a simple Flink program,
 * <li>write and use user-defined functions. 
 * </ul>
 */
public class PojoExample {
	
	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		// Checking input parameters
		final ParameterTool params = ParameterTool.fromArgs(args);
		System.out.println("Usage: PojoExample --input <path> --output <path>");

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);

		// get input data
		DataStream<String> text;
		if (params.has("input")) {
			System.out.println("Executing WordCountPojo example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// read the text file from given input path
			text = env.readTextFile(params.get("input"));
		} else {
			// get default test text data
			text = env.fromElements(WordCountData.WORDS);
		}

		DataStream<Word> counts =
		// split up the lines into Word objects
		text.flatMap(new Tokenizer())
		// group by the field word and sum up the frequency
				.keyBy("word").sum("frequency");

		if (params.has("output")) {
			counts.writeAsText(params.get("output"));
		} else {
			System.out.println("Printing result to stdout. Use --output to specify output path.");
			counts.print();
		}

		// execute program
		env.execute("WordCount Pojo Example");
	}

	// *************************************************************************
	// DATA TYPES
	// *************************************************************************

	/**
	 * This is the POJO (Plain Old Java Object) that is being used for all the
	 * operations. As long as all fields are public or have a getter/setter, the
	 * system can handle them
	 */
	public static class Word {

		private String word;
		private Integer frequency;

		public Word() {
		}

		public Word(String word, int i) {
			this.word = word;
			this.frequency = i;
		}

		public String getWord() {
			return word;
		}

		public void setWord(String word) {
			this.word = word;
		}

		public Integer getFrequency() {
			return frequency;
		}

		public void setFrequency(Integer frequency) {
			this.frequency = frequency;
		}

		@Override
		public String toString() {
			return "(" + word + "," + frequency + ")";
		}
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Word> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String value, Collector<Word> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Word(token, 1));
				}
			}
		}
	}

}
