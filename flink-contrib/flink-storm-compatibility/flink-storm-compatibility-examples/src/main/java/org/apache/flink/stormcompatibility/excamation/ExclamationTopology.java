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

package org.apache.flink.stormcompatibility.excamation;

import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.stormcompatibility.api.FlinkTopologyBuilder;
import org.apache.flink.stormcompatibility.excamation.stormoperators.ExclamationBolt;
import org.apache.flink.stormcompatibility.util.FiniteStormFileSpout;
import org.apache.flink.stormcompatibility.util.FiniteStormInMemorySpout;
import org.apache.flink.stormcompatibility.util.OutputFormatter;
import org.apache.flink.stormcompatibility.util.SimpleOutputFormatter;
import org.apache.flink.stormcompatibility.util.StormBoltFileSink;
import org.apache.flink.stormcompatibility.util.StormBoltPrintSink;

/**
 * Implements the "Exclamation" program that attaches five exclamation mark to every line of a text
 * files in a streaming fashion. The program is constructed as a regular {@link StormTopology}.
 * <p/>
 * <p/>
 * The input is a plain text file with lines separated by newline characters.
 * <p/>
 * <p/>
 * Usage: <code>StormExclamation[Local|RemoteByClient|RemoteBySubmitter] &lt;text path&gt;
 * &lt;result path&gt;</code><br/>
 * If no parameters are provided, the program is run with default data from
 * {@link WordCountData}.
 * <p/>
 * <p/>
 * This example shows how to:
 * <ul>
 * <li>construct a regular Storm topology as Flink program</li>
 * <li>make use of the FiniteStormSpout interface</li>
 * </ul>
 */
public class ExclamationTopology {

	public final static String spoutId = "source";
	public final static String firstBoltId = "exclamation1";
	public final static String secondBoltId = "exclamation2";
	public final static String sinkId = "sink";
	private final static OutputFormatter formatter = new SimpleOutputFormatter();

	public static FlinkTopologyBuilder buildTopology() {
		final FlinkTopologyBuilder builder = new FlinkTopologyBuilder();

		// get input data
		if (fileInputOutput) {
			// read the text file from given input path
			final String[] tokens = textPath.split(":");
			final String inputFile = tokens[tokens.length - 1];
			builder.setSpout(spoutId, new FiniteStormFileSpout(inputFile));
		} else {
			builder.setSpout(spoutId, new FiniteStormInMemorySpout(WordCountData.WORDS));
		}

		builder.setBolt(firstBoltId, new ExclamationBolt(), 3).shuffleGrouping(spoutId);
		builder.setBolt(secondBoltId, new ExclamationBolt(), 2).shuffleGrouping(firstBoltId);

		// emit result
		if (fileInputOutput) {
			// read the text file from given input path
			final String[] tokens = outputPath.split(":");
			final String outputFile = tokens[tokens.length - 1];
			builder.setBolt(sinkId, new StormBoltFileSink(outputFile, formatter))
					.shuffleGrouping(secondBoltId);
		} else {
			builder.setBolt(sinkId, new StormBoltPrintSink(formatter), 4)
					.shuffleGrouping(secondBoltId);
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
				System.err.println(
						"Usage: StormExclamation[Local|RemoteByClient|RemoteBySubmitter] <text " +
								"path> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing StormExclamation example with built-in default data");
			System.out.println("  Provide parameters to read input data from a file");
			System.out.println(
					"  Usage: StormExclamation[Local|RemoteByClient|RemoteBySubmitter] <text path>" +
							" <result path>");
		}

		return true;
	}

}
