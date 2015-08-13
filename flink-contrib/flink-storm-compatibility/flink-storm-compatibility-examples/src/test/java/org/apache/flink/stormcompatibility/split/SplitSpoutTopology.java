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
package org.apache.flink.stormcompatibility.split;

import org.apache.flink.stormcompatibility.api.FlinkTopologyBuilder;
import org.apache.flink.stormcompatibility.split.stormoperators.RandomSpout;
import org.apache.flink.stormcompatibility.split.stormoperators.VerifyAndEnrichBolt;
import org.apache.flink.stormcompatibility.util.OutputFormatter;
import org.apache.flink.stormcompatibility.util.StormBoltFileSink;
import org.apache.flink.stormcompatibility.util.StormBoltPrintSink;
import org.apache.flink.stormcompatibility.util.TupleOutputFormatter;

public class SplitSpoutTopology {
	public final static String spoutId = "randomSplitSource";
	public final static String evenVerifierId = "evenVerifier";
	public final static String oddVerifierId = "oddVerifier";
	public final static String sinkId = "sink";
	private final static OutputFormatter formatter = new TupleOutputFormatter();

	public static FlinkTopologyBuilder buildTopology() {
		final FlinkTopologyBuilder builder = new FlinkTopologyBuilder();

		builder.setSpout(spoutId, new RandomSpout(true, seed));
		builder.setBolt(evenVerifierId, new VerifyAndEnrichBolt(true)).shuffleGrouping(spoutId,
				RandomSpout.EVEN_STREAM);
		builder.setBolt(oddVerifierId, new VerifyAndEnrichBolt(false)).shuffleGrouping(spoutId,
				RandomSpout.ODD_STREAM);

		// emit result
		if (outputPath != null) {
			// read the text file from given input path
			final String[] tokens = outputPath.split(":");
			final String outputFile = tokens[tokens.length - 1];
			builder.setBolt(sinkId, new StormBoltFileSink(outputFile, formatter))
			.shuffleGrouping(evenVerifierId).shuffleGrouping(oddVerifierId);
		} else {
			builder.setBolt(sinkId, new StormBoltPrintSink(formatter), 4)
			.shuffleGrouping(evenVerifierId).shuffleGrouping(oddVerifierId);
		}

		return builder;
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static long seed = System.currentTimeMillis();
	private static String outputPath = null;

	static boolean parseParameters(final String[] args) {

		if (args.length > 0) {
			// parse input arguments
			if (args.length == 2) {
				seed = Long.parseLong(args[0]);
				outputPath = args[1];
			} else {
				System.err.println("Usage: SplitSpoutTopology <seed> <result path>");
				return false;
			}
		} else {
			System.out.println("Executing SplitSpoutTopology example with random data");
			System.out.println("  Usage: SplitSpoutTopology <seed> <result path>");
		}

		return true;
	}

}
