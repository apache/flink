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

package org.apache.flink.streaming.examples.twitter;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.json.JSONParseFlatMap;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.streaming.examples.twitter.util.TwitterStreamData;
import org.apache.flink.util.Collector;
import org.apache.sling.commons.json.JSONException;

import java.util.StringTokenizer;

/**
 * Implements the "TwitterStream" program that computes a most used word
 * occurrence over JSON files in a streaming fashion.
 * <p/>
 * <p/>
 * The input is a JSON text file with lines separated by newline characters.
 * <p/>
 * <p/>
 * Usage: <code>TwitterStream &lt;text path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link TwitterStreamData}.
 * <p/>
 * <p/>
 * This example shows how to:
 * <ul>
 * <li>acquire external data,
 * <li>use in-line defined functions,
 * <li>handle flattened stream inputs.
 * </ul>
 */
public class TwitterStream {

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {
		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataStream<String> streamSource = getTextDataStream(env);

		DataStream<Tuple2<String, Integer>> tweets = streamSource
				// selecting English tweets and splitting to (word, 1)
				.flatMap(new SelectEnglishAndTokenizeFlatMap())
				// group by words and sum their occurrences
				.groupBy(0).sum(1);

		// emit result
		if (fileOutput) {
			tweets.writeAsText(outputPath);
		} else {
			tweets.print();
		}

		// execute program
		env.execute("Twitter Streaming Example");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Makes sentences from English tweets.
	 * <p/>
	 * <p>
	 * Implements a string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" (Tuple2<String,
	 * Integer>).
	 * </p>
	 */
	public static class SelectEnglishAndTokenizeFlatMap extends JSONParseFlatMap<String, Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;

		/**
		 * Select the language from the incoming JSON text
		 */
		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
			try {
				if (getString(value, "user.lang").equals("en")) {
					// message of tweet
					StringTokenizer tokenizer = new StringTokenizer(getString(value, "text"));

					// split the message
					while (tokenizer.hasMoreTokens()) {
						String result = tokenizer.nextToken().replaceAll("\\s*", "").toLowerCase();

						if (result != null && !result.equals("")) {
							out.collect(new Tuple2<String, Integer>(result, 1));
						}
					}
				}
			} catch (JSONException e) {
				// the JSON was not parsed correctly
			}
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileInput = false;
	private static boolean fileOutput = false;
	private static String propertiesPath;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {
		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;
			if (args.length == 2) {
				fileInput = true;
				propertiesPath = args[0];
				outputPath = args[1];
			} else if (args.length == 1) {
				outputPath = args[0];
			} else {
				System.err.println("USAGE:\nTwitterStream [<pathToPropertiesFile>] <result path>");
				return false;
			}
		} else {
			System.out.println("Executing TwitterStream example with built-in default data.");
			System.out.println("  Provide parameters to read input data from a file.");
			System.out.println("  USAGE: TwitterStream [<pathToPropertiesFile>] <result path>");
		}
		return true;
	}

	private static DataStream<String> getTextDataStream(StreamExecutionEnvironment env) {
		if (fileInput) {
			// read the text file from given input path
			return env.addSource(new TwitterSource(propertiesPath));
		} else {
			// get default test text data
			return env.fromElements(TwitterStreamData.TEXTS);
		}
	}
}
