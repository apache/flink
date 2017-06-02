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

import org.apache.flink.storm.api.FlinkLocalCluster;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.flink.storm.exclamation.operators.ExclamationBolt;

import org.apache.storm.Config;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Implements the "Exclamation" program that attaches five exclamation mark to every line of a text files in a streaming
 * fashion. The program is constructed as a regular {@link org.apache.storm.generated.StormTopology} and submitted to
 * Flink for execution in the same way as to a Storm {@link org.apache.storm.LocalCluster}.
 *
 * <p>This example shows how to run program directly within Java, thus it cannot be used to submit a
 * {@link org.apache.storm.generated.StormTopology} via Flink command line clients (ie, bin/flink).
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>ExclamationLocal &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link org.apache.flink.examples.java.wordcount.util.WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>run a regular Storm program locally on Flink</li>
 * </ul>
 */
public class ExclamationLocal {

	public static final String TOPOLOGY_ID = "Streaming Exclamation";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws Exception {

		if (!ExclamationTopology.parseParameters(args)) {
			return;
		}

		// build Topology the Storm way
		final TopologyBuilder builder = ExclamationTopology.buildTopology();

		// execute program locally
		Config conf = new Config();
		conf.put(ExclamationBolt.EXCLAMATION_COUNT, ExclamationTopology.getExclamation());
		conf.put(FlinkLocalCluster.SUBMIT_BLOCKING, true); // only required to stabilize integration test

		final FlinkLocalCluster cluster = FlinkLocalCluster.getLocalCluster();
		cluster.submitTopology(TOPOLOGY_ID, conf, FlinkTopology.createTopology(builder));
		cluster.shutdown();
	}

}
