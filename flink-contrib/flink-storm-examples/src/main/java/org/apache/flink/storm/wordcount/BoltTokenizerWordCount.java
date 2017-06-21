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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.storm.wordcount.operators.BoltTokenizer;
import org.apache.flink.storm.wordcount.util.WordCountData;
import org.apache.flink.storm.wrappers.BoltWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.storm.topology.IRichBolt;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text files in a streaming
 * fashion. The tokenizer step is performed by a {@link IRichBolt Bolt}.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>use a Bolt within a Flink Streaming program.</li>
 * </ul>
 */
public class BoltTokenizerWordCount {

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

		final DataStream<Tuple2<String, Integer>> counts = text
				// split up the lines in pairs (2-tuples) containing: (word,1)
				// this is done by a bolt that is wrapped accordingly
				.transform("BoltTokenizer",
						TypeExtractor.getForObject(new Tuple2<String, Integer>("", 0)),
						new BoltWrapper<String, Tuple2<String, Integer>>(new BoltTokenizer()))
				// group by the tuple field "0" and sum up tuple field "1"
				.keyBy(0).sum(1);

		// emit result
		if (fileOutput) {
			counts.writeAsText(outputPath);
		} else {
			counts.print();
		}

		// execute program
		env.execute("Streaming WordCount with bolt tokenizer");
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
				System.err.println("Usage: BoltTokenizerWordCount <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing BoltTokenizerWordCount example with built-in default data");
			System.out.println("  Provide parameters to read input data from a file");
			System.out.println("  Usage: BoltTokenizerWordCount <text path> <result path>");
		}
		return true;
	}

	private static DataStream<String> getTextDataStream(final StreamExecutionEnvironment env) {
		if (fileOutput) {
			// read the text file from given input path
			return env.readTextFile(textPath);
		}

		return env.fromElements(WordCountData.WORDS);
	}

}
