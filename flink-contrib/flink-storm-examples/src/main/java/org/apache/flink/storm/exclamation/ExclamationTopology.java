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

package org.apache.flink.storm.exclamation;

import org.apache.flink.storm.exclamation.operators.ExclamationBolt;
import org.apache.flink.storm.util.BoltFileSink;
import org.apache.flink.storm.util.BoltPrintSink;
import org.apache.flink.storm.util.FiniteFileSpout;
import org.apache.flink.storm.util.FiniteInMemorySpout;
import org.apache.flink.storm.util.OutputFormatter;
import org.apache.flink.storm.util.SimpleOutputFormatter;
import org.apache.flink.storm.wordcount.util.WordCountData;

import org.apache.storm.topology.TopologyBuilder;

/**
 * Implements the "Exclamation" program that attaches two exclamation marks to every line of a text files in a streaming
 * fashion. The program is constructed as a regular {@link org.apache.storm.generated.StormTopology}.
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>Exclamation[Local|RemoteByClient|RemoteBySubmitter] &lt;text path&gt;
 * &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>construct a regular Storm topology as Flink program</li>
 * <li>make use of the FiniteSpout interface</li>
 * </ul>
 */
public class ExclamationTopology {

	private static final String spoutId = "source";
	private static final String firstBoltId = "exclamation1";
	private static final String secondBoltId = "exclamation2";
	private static final String sinkId = "sink";
	private static final OutputFormatter formatter = new SimpleOutputFormatter();

	public static TopologyBuilder buildTopology() {
		final TopologyBuilder builder = new TopologyBuilder();

		// get input data
		if (fileInputOutput) {
			// read the text file from given input path
			final String[] tokens = textPath.split(":");
			final String inputFile = tokens[tokens.length - 1];
			builder.setSpout(spoutId, new FiniteFileSpout(inputFile));
		} else {
			builder.setSpout(spoutId, new FiniteInMemorySpout(WordCountData.WORDS));
		}

		builder.setBolt(firstBoltId, new ExclamationBolt(), 3).shuffleGrouping(spoutId);
		builder.setBolt(secondBoltId, new ExclamationBolt(), 2).shuffleGrouping(firstBoltId);

		// emit result
		if (fileInputOutput) {
			// read the text file from given input path
			final String[] tokens = outputPath.split(":");
			final String outputFile = tokens[tokens.length - 1];
			builder.setBolt(sinkId, new BoltFileSink(outputFile, formatter))
			.shuffleGrouping(secondBoltId);
		} else {
			builder.setBolt(sinkId, new BoltPrintSink(formatter), 4)
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
	private static int exclamationNum = 3;

	static int getExclamation() {
		return exclamationNum;
	}

	static boolean parseParameters(final String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileInputOutput = true;
			if (args.length == 3) {
				textPath = args[0];
				outputPath = args[1];
				exclamationNum = Integer.parseInt(args[2]);
			} else {
				System.err.println("Usage: StormExclamation* <text path> <result path>  <number of exclamation marks>");
				return false;
			}
		} else {
			System.out.println("Executing StormExclamation example with built-in default data");
			System.out.println("  Provide parameters to read input data from a file");
			System.out.println("  Usage: StormExclamation <text path> <result path> <number of exclamation marks>");
		}

		return true;
	}

}
