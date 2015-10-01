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

package org.apache.flink.stormcompatibility.excamation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.stormcompatibility.excamation.stormoperators.ExclamationBolt;
import org.apache.flink.stormcompatibility.util.StormConfig;
import org.apache.flink.stormcompatibility.wrappers.StormBoltWrapper;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import backtype.storm.utils.Utils;

/**
 * Implements the "Exclamation" program that attaches five exclamation mark to every line of a text
 * files in a streaming fashion. The program is constructed as a regular {@link StormTopology}.
 * <p/>
 * <p/>
 * The input is a plain text file with lines separated by newline characters.
 * <p/>
 * <p/>
 * Usage: <code>StormExclamationWithStormBolt &lt;text path&gt; &lt;result path&gt; &lt;number of exclamation marks&gt;</code><br/>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 * <p/>
 * <p/>
 * This example shows how to:
 * <ul>
 * <li>use a Storm bolt within a Flink Streaming program</li>
 * </ul>
 */
public class ExclamationWithStormBolt {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// set Storm configuration
		StormConfig config = new StormConfig();
		config.put(ExclamationBolt.EXCLAMATION_COUNT, new Integer(exclamationNum));
		env.getConfig().setGlobalJobParameters(config);

		// get input data
		final DataStream<String> text = getTextDataStream(env);

		final DataStream<String> exclaimed = text
				.transform("StormBoltTokenizer",
						TypeExtractor.getForObject(""),
						new StormBoltWrapper<String, String>(new ExclamationBolt(),
								new String[] { Utils.DEFAULT_STREAM_ID }))
				.map(new ExclamationMap());

		// emit result
		if (fileOutput) {
			exclaimed.writeAsText(outputPath);
		} else {
			exclaimed.print();
		}

		// execute program
		env.execute("Streaming WordCount with Storm bolt tokenizer");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	private static class ExclamationMap implements MapFunction<String, String> {
		private static final long serialVersionUID = 4614754344067170619L;

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
	private static int exclamationNum = 3;

	private static boolean parseParameters(final String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length == 3) {
				textPath = args[0];
				outputPath = args[1];
				exclamationNum = Integer.parseInt(args[2]);
			} else {
				System.err.println("Usage: ExclamationWithStormBolt <text path> <result path> <number of exclamation marks>");
				return false;
			}
		} else {
			System.out.println("Executing ExclamationWithStormBolt example with built-in default data");
			System.out.println("  Provide parameters to read input data from a file");
			System.out.println("  Usage: ExclamationWithStormBolt <text path> <result path> <number of exclamation marks>");
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
