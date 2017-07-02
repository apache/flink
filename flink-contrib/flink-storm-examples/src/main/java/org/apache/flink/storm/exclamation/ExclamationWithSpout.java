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

package org.apache.flink.storm.exclamation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.storm.util.FiniteFileSpout;
import org.apache.flink.storm.util.FiniteInMemorySpout;
import org.apache.flink.storm.util.StormConfig;
import org.apache.flink.storm.wordcount.util.WordCountData;
import org.apache.flink.storm.wrappers.SpoutWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.storm.utils.Utils;

/**
 * Implements the "Exclamation" program that attaches six exclamation marks to every line of a text files in a streaming
 * fashion. The program is constructed as a regular {@link org.apache.storm.generated.StormTopology}.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>ExclamationWithSpout &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>use a Storm spout within a Flink Streaming program</li>
 * <li>make use of the FiniteSpout interface</li>
 * <li>make use of the FiniteSpout interface</li>
 * <li>how to configure a Spout using StormConfig</li>
 * </ul>
 */
public class ExclamationWithSpout {

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

		final DataStream<String> exclaimed = text
				.map(new ExclamationMap())
				.map(new ExclamationMap());

		// emit result
		if (fileOutput) {
			exclaimed.writeAsText(outputPath);
		} else {
			exclaimed.print();
		}

		// execute program
		env.execute("Streaming Exclamation with Storm spout source");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	private static class ExclamationMap implements MapFunction<String, String> {
		private static final long serialVersionUID = -684993133807698042L;

		@Override
		public String map(String value) throws Exception {
			return value + "!!!";
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
				System.err.println("Usage: ExclamationWithSpout <text path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing ExclamationWithSpout example with built-in default data");
			System.out.println("  Provide parameters to read input data from a file");
			System.out.println("  Usage: ExclamationWithSpout <text path> <result path>");
		}
		return true;
	}

	private static DataStream<String> getTextDataStream(final StreamExecutionEnvironment env) {
		if (fileOutput) {
			final String[] tokens = textPath.split(":");
			final String inputFile = tokens[tokens.length - 1];

			// set Storm configuration
			StormConfig config = new StormConfig();
			config.put(FiniteFileSpout.INPUT_FILE_PATH, inputFile);
			env.getConfig().setGlobalJobParameters(config);

			return env.addSource(
					new SpoutWrapper<String>(new FiniteFileSpout(),
							new String[] { Utils.DEFAULT_STREAM_ID }),
							TypeExtractor.getForClass(String.class)).setParallelism(1);
		}

		return env.addSource(
				new SpoutWrapper<String>(new FiniteInMemorySpout(
						WordCountData.WORDS), new String[] { Utils.DEFAULT_STREAM_ID }),
						TypeExtractor.getForClass(String.class)).setParallelism(1);

	}

}
