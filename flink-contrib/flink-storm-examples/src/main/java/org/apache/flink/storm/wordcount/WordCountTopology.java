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

import org.apache.flink.storm.util.BoltFileSink;
import org.apache.flink.storm.util.BoltPrintSink;
import org.apache.flink.storm.util.NullTerminatingSpout;
import org.apache.flink.storm.util.OutputFormatter;
import org.apache.flink.storm.util.TupleOutputFormatter;
import org.apache.flink.storm.wordcount.operators.BoltCounter;
import org.apache.flink.storm.wordcount.operators.BoltCounterByName;
import org.apache.flink.storm.wordcount.operators.BoltTokenizer;
import org.apache.flink.storm.wordcount.operators.BoltTokenizerByName;
import org.apache.flink.storm.wordcount.operators.WordCountFileSpout;
import org.apache.flink.storm.wordcount.operators.WordCountInMemorySpout;
import org.apache.flink.storm.wordcount.util.WordCountData;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text files in a streaming
 * fashion. The program is constructed as a regular {@link StormTopology}.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage:
 * <code>WordCount[Local|LocalByName|RemoteByClient|RemoteBySubmitter] &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>how to construct a regular Storm topology as Flink program</li>
 * </ul>
 */
public class WordCountTopology {
	private static final String spoutId = "source";
	private static final String tokenierzerId = "tokenizer";
	private static final String counterId = "counter";
	private static final String sinkId = "sink";
	private static final OutputFormatter formatter = new TupleOutputFormatter();

	public static TopologyBuilder buildTopology() {
		return buildTopology(true);
	}

	public static TopologyBuilder buildTopology(boolean indexOrName) {

		final TopologyBuilder builder = new TopologyBuilder();

		// get input data
		if (fileInputOutput) {
			// read the text file from given input path
			final String[] tokens = textPath.split(":");
			final String inputFile = tokens[tokens.length - 1];
			// inserting NullTerminatingSpout only required to stabilize integration test
			builder.setSpout(spoutId, new NullTerminatingSpout(new WordCountFileSpout(inputFile)));
		} else {
			builder.setSpout(spoutId, new WordCountInMemorySpout());
		}

		if (indexOrName) {
			// split up the lines in pairs (2-tuples) containing: (word,1)
			builder.setBolt(tokenierzerId, new BoltTokenizer(), 4).shuffleGrouping(spoutId);
			// group by the tuple field "0" and sum up tuple field "1"
			builder.setBolt(counterId, new BoltCounter(), 4).fieldsGrouping(tokenierzerId,
					new Fields(BoltTokenizer.ATTRIBUTE_WORD));
		} else {
			// split up the lines in pairs (2-tuples) containing: (word,1)
			builder.setBolt(tokenierzerId, new BoltTokenizerByName(), 4).shuffleGrouping(
					spoutId);
			// group by the tuple field "0" and sum up tuple field "1"
			builder.setBolt(counterId, new BoltCounterByName(), 4).fieldsGrouping(
					tokenierzerId, new Fields(BoltTokenizerByName.ATTRIBUTE_WORD));
		}

		// emit result
		if (fileInputOutput) {
			// read the text file from given input path
			final String[] tokens = outputPath.split(":");
			final String outputFile = tokens[tokens.length - 1];
			builder.setBolt(sinkId, new BoltFileSink(outputFile, formatter)).shuffleGrouping(counterId);
		} else {
			builder.setBolt(sinkId, new BoltPrintSink(formatter), 4).shuffleGrouping(counterId);
		}

		return builder;
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileInputOutput = false;
	private static String textPath;
	private static String outputPath;

	static boolean parseParameters(final String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileInputOutput = true;
			if (args.length == 2) {
				textPath = args[0];
				outputPath = args[1];
			} else {
				System.err.println("Usage: WordCount* <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing WordCount example with built-in default data");
			System.out.println("  Provide parameters to read input data from a file");
			System.out.println("  Usage: WordCount* <text path> <result path>");
		}

		return true;
	}

}
