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

package org.apache.flink.storm.wordcount;

import org.apache.flink.storm.api.FlinkClient;
import org.apache.flink.storm.api.FlinkSubmitter;
import org.apache.flink.storm.api.FlinkTopology;
import org.apache.flink.storm.wordcount.util.WordCountData;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text files in a streaming
 * fashion. The program is constructed as a regular {@link StormTopology} and submitted to Flink for execution in the
 * same way as to a Storm cluster similar to {@link StormSubmitter}. The Flink cluster can be local or remote.
 *
 * <p>This example shows how to submit the program via Java as well as Flink's command line client (ie, bin/flink).
 *
 * <p>The input is a plain text file with lines separated by newline characters.
 *
 * <p>Usage: <code>WordCountRemoteBySubmitter &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 *
 * <p>This example shows how to:
 * <ul>
 * <li>submit a regular Storm program to a local or remote Flink cluster.</li>
 * </ul>
 */
public class WordCountRemoteBySubmitter {
	private static final String topologyId = "Storm WordCount";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws Exception {

		if (!WordCountTopology.parseParameters(args)) {
			return;
		}

		// build Topology the Storm way
		final TopologyBuilder builder = WordCountTopology.buildTopology();

		// execute program on Flink cluster
		final Config conf = new Config();
		// We can set Jobmanager host/port values manually or leave them blank
		// if not set and
		// - executed within Java, default values "localhost" and "6123" are set by FlinkSubmitter
		// - executed via bin/flink values from flink-conf.yaml are set by FlinkSubmitter.
		// conf.put(Config.NIMBUS_HOST, "localhost");
		// conf.put(Config.NIMBUS_THRIFT_PORT, new Integer(6123));

		// The user jar file must be specified via JVM argument if executed via Java.
		// => -Dstorm.jar=target/WordCount-StormTopology.jar
		// If bin/flink is used, the jar file is detected automatically.
		FlinkSubmitter.submitTopology(topologyId, conf, FlinkTopology.createTopology(builder));

		Thread.sleep(5 * 1000);

		FlinkClient.getConfiguredClient(conf).killTopology(topologyId);
	}

}
