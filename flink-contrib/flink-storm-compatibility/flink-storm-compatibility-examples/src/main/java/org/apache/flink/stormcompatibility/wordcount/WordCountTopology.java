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

package org.apache.flink.stormcompatibility.wordcount;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.stormcompatibility.api.FlinkTopologyBuilder;
import org.apache.flink.stormcompatibility.util.OutputFormatter;
import org.apache.flink.stormcompatibility.util.StormBoltFileSink;
import org.apache.flink.stormcompatibility.util.StormBoltPrintSink;
import org.apache.flink.stormcompatibility.util.StormFileSpout;
import org.apache.flink.stormcompatibility.util.StormInMemorySpout;
import org.apache.flink.stormcompatibility.util.TupleOutputFormatter;
import org.apache.flink.stormcompatibility.wordcount.stormoperators.StormBoltCounter;
import org.apache.flink.stormcompatibility.wordcount.stormoperators.StormBoltTokenizer;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text files in a streaming
 * fashion. The program is constructed as a regular {@link StormTopology}.
 * <p/>
 * <p/>
 * The input is a plain text file with lines separated by newline characters.
 * <p/>
 * <p/>
 * Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 * <p/>
 * <p/>
 * This example shows how to:
 * <ul>
 * <li>how to construct a regular Storm topology as Flink program
 * </ul>
 */
public class WordCountTopology {
	public final static String spoutId = "source";
	public final static String tokenierzerId = "tokenizer";
	public final static String counterId = "counter";
	public final static String sinkId = "sink";
	private final static OutputFormatter formatter = new TupleOutputFormatter();

	public static FlinkTopologyBuilder buildTopology() {

		final FlinkTopologyBuilder builder = new FlinkTopologyBuilder();

		// get input data
		if (fileInputOutput) {
			// read the text file from given input path
			final String[] tokens = textPath.split(":");
			final String inputFile = tokens[tokens.length - 1];
			builder.setSpout(spoutId, new StormFileSpout(inputFile));
		} else {
			builder.setSpout(spoutId, new StormInMemorySpout(WordCountData.WORDS));
		}

		// split up the lines in pairs (2-tuples) containing: (word,1)
		builder.setBolt(tokenierzerId, new StormBoltTokenizer(), 4).shuffleGrouping(spoutId);
		// group by the tuple field "0" and sum up tuple field "1"
		builder.setBolt(counterId, new StormBoltCounter(), 4).fieldsGrouping(tokenierzerId,
				new Fields(StormBoltTokenizer.ATTRIBUTE_WORD));

		// emit result
		if (fileInputOutput) {
			// read the text file from given input path
			final String[] tokens = outputPath.split(":");
			final String outputFile = tokens[tokens.length - 1];
			builder.setBolt(sinkId, new StormBoltFileSink(outputFile, formatter)).shuffleGrouping(counterId);
		} else {
			builder.setBolt(sinkId, new StormBoltPrintSink(formatter), 4).shuffleGrouping(counterId);
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
				System.err.println("Usage: StormWordCount* <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing StormWordCount* example with built-in default data");
			System.out.println("  Provide parameters to read input data from a file");
			System.out.println("  Usage: StormWordCount* <text path> <result path>");
		}

		return true;
	}

}
