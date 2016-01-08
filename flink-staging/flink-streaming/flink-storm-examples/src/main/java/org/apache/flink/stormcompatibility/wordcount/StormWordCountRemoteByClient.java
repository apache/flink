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

import org.apache.flink.examples.java.wordcount.util.WordCountData;
import org.apache.flink.stormcompatibility.api.FlinkClient;
import org.apache.flink.stormcompatibility.api.FlinkTopologyBuilder;

import backtype.storm.Config;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.generated.StormTopology;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;





/**
 * Implements the "WordCount" program that computes a simple word occurrence histogram over text files in a streaming
 * fashion. The program is constructed as a regular {@link StormTopology} and submitted to Flink for execution in the
 * same way as to a Storm cluster similar to {@link NimbusClient}. The Flink cluster can be local or remote.
 * 
 * This example shows how to submit the program via Java, thus it cannot be used to submit a {@link StormTopology} via
 * Flink command line clients (ie, bin/flink).
 * 
 * <p>
 * The input is a plain text file with lines separated by newline characters.
 * 
 * <p>
 * Usage: <code>WordCount &lt;text path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link WordCountData}.
 * 
 * <p>
 * This example shows how to:
 * <ul>
 * <li>submit a regular Storm program to a local or remote Flink cluster.
 * </ul>
 */
public class StormWordCountRemoteByClient {
	public final static String topologyId = "Streaming WordCount";
	private final static String uploadedJarLocation = "target/flink-storm-examples-0.9-SNAPSHOT-WordCountStorm.jar";
	
	// *************************************************************************
	// PROGRAM
	// *************************************************************************
	
	public static void main(final String[] args) throws AlreadyAliveException, InvalidTopologyException,
		NotAliveException {
		
		if(!WordCountTopology.parseParameters(args)) {
			return;
		}
		
		// build Topology the Storm way
		final FlinkTopologyBuilder builder = WordCountTopology.buildTopology();
		
		// execute program on Flink cluster
		final Config conf = new Config();
		conf.put(Config.NIMBUS_HOST, "localhost"); // can be changed to remote address
		conf.put(Config.NIMBUS_THRIFT_PORT, new Integer(6123)); // use default flink jobmanger.rpc.port
		
		final FlinkClient cluster = FlinkClient.getConfiguredClient(conf);
		cluster.submitTopology(topologyId, uploadedJarLocation, null, builder.createTopology());
		
		Utils.sleep(5 * 1000);
		
		cluster.killTopology(topologyId);
	}
	
}
