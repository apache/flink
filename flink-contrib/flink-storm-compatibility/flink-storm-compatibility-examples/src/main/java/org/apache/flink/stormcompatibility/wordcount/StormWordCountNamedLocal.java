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

import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.Utils;
import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.stormcompatibility.api.FlinkLocalCluster;
import org.apache.flink.stormcompatibility.api.FlinkTopologyBuilder;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text files in a streaming
 * fashion. The program is constructed as a regular {@link StormTopology} and submitted to Flink for execution in the
 * same way as to a Storm {@link LocalCluster}. In contrast to {@link StormWordCountLocal} all bolts access the field of
 * input tuples by name instead of index.
 * <p/>
 * This example shows how to run program directly within Java, thus it cannot be used to submit a {@link StormTopology}
 * via Flink command line clients (ie, bin/flink).
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
 * <li>run a regular Storm program locally on Flink
 * </ul>
 */
public class StormWordCountNamedLocal {
	public final static String topologyId = "Streaming WordCountName";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws Exception {

		if (!WordCountTopology.parseParameters(args)) {
			return;
		}

		// build Topology the Storm way
		final FlinkTopologyBuilder builder = WordCountTopology.buildTopology(false);

		// execute program locally
		final FlinkLocalCluster cluster = FlinkLocalCluster.getLocalCluster();
		cluster.submitTopology(topologyId, null, builder.createTopology());

		Utils.sleep(10 * 1000);

		// TODO kill does no do anything so far
		cluster.killTopology(topologyId);
		cluster.shutdown();
	}

}
