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

package org.apache.flink.streaming.examples.socket;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.examples.wordcount.WordCount.Tokenizer;

/**
 * This example shows an implementation of WordCount with data from a text
 * socket. To run the example make sure that the service providing the text data
 * is already up and running.
 * <p>
 * To start an example socket text stream on your local machine run netcat from
 * a command line: <code>nc -lk 9999</code>, where the parameter specifies the
 * port number.
 * </p>
 * <p>
 * Usage:
 * <code>SocketTextStreamWordCount &lt;hostname&gt; &lt;port&gt; &lt;result path&gt;</code>
 * </p>
 * <p>
 * This example shows how to:
 * <ul>
 * <li>use StreamExecutionEnvironment.socketTextStream
 * <li>write a simple Flink program,
 * <li>write and use user-defined functions.
 * </ul>
 *
 * @see <a href="www.openbsd.org/cgi-bin/man.cgi?query=nc">netcat</a>
 */
public class SocketTextStreamWordCount {
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment
				.getExecutionEnvironment();

		// get input data
		DataStream<String> text = env.socketTextStream(hostName, port, '\n', 0);

		DataStream<Tuple2<String, Integer>> counts =
				// split up the lines in pairs (2-tuples) containing: (word,1)
				text.flatMap(new Tokenizer())
						// group by the tuple field "0" and sum up tuple field "1"
						.keyBy(0)
						.sum(1);

		if (fileOutput) {
			counts.writeAsText(outputPath);
		} else {
			counts.print();
		}

		// execute program
		env.execute("WordCount from SocketTextStream Example");
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String hostName;
	private static int port;
	private static String outputPath;

	private static boolean parseParameters(String[] args) {

		// parse input arguments
		if (args.length == 3) {
			fileOutput = true;
			hostName = args[0];
			port = Integer.valueOf(args[1]);
			outputPath = args[2];
		} else if (args.length == 2) {
			hostName = args[0];
			port = Integer.valueOf(args[1]);
		} else {
			System.err.println("Usage: SocketTextStreamWordCount <hostname> <port> [<output path>]");
			return false;
		}
		return true;
	}
}
