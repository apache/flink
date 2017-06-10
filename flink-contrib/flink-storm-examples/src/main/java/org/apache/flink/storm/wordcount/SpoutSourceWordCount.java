/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.storm.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.storm.wordcount.operators.WordCountFileSpout;
import org.apache.flink.storm.wordcount.operators.WordCountInMemorySpout;
import org.apache.flink.storm.wrappers.SpoutWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import org.apache.storm.topology.IRichSpout;
import org.apache.storm.utils.Utils;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text files in a streaming
 * fashion. The used data source is a {@link IRichSpout Spout}.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>use a Spout within a Flink Streaming program.</li>
 * </ul>
 */
public class SpoutSourceWordCount {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		final DataStream<String> text = getTextDataStream(env);

		final DataStream<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer())
				// group by the tuple field "0" and sum up tuple field "1"
				.keyBy(0).sum(1);

		// emit result
		if (fileOutput) {
			counts.writeAsText(outputPath);
		} else {
			counts.print();
		}

		// execute program
		env.execute("Streaming WordCount with spout source");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Implements the string tokenizer that splits sentences into words as a user-defined FlatMapFunction. The function
	 * takes a line (String) and splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String, Integer>}).
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(final String value, final Collector<Tuple2<String, Integer>> out) throws Exception {
			// normalize and split the line
			final String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (final String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String textPath;
	private static String outputPath;

	private static boolean parseParameters(final String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err.println("Usage: SpoutSourceWordCount <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing SpoutSourceWordCount example with built-in default data");
			System.out.println("  Provide parameters to read input data from a file");
			System.out.println("  Usage: SpoutSourceWordCount <text path> <result path>");
		}
		return true;
	}

	private static DataStream<String> getTextDataStream(final StreamExecutionEnvironment env) {
		if (fileOutput) {
			// read the text file from given input path
			final String[] tokens = textPath.split(":");
			final String localFile = tokens[tokens.length - 1];
			return env.addSource(
					new SpoutWrapper<String>(new WordCountFileSpout(localFile),
							new String[] { Utils.DEFAULT_STREAM_ID }, -1),
							TypeExtractor.getForClass(String.class)).setParallelism(1);
		}

		return env.addSource(
				new SpoutWrapper<String>(new WordCountInMemorySpout(),
						new String[] { Utils.DEFAULT_STREAM_ID }, -1),
						TypeExtractor.getForClass(String.class)).setParallelism(1);

	}

}
