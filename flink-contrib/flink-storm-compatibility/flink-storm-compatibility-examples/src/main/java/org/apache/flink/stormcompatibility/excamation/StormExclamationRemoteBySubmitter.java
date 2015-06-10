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

import backtype.storm.Config;
import org.apache.flink.stormcompatibility.api.FlinkClient;
import org.apache.flink.stormcompatibility.api.FlinkSubmitter;
import org.apache.flink.stormcompatibility.api.FlinkTopologyBuilder;

public class StormExclamationRemoteBySubmitter {

	public final static String topologyId = "Streaming Exclamation";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws Exception {

		if (!ExclamationTopology.parseParameters(args)) {
			return;
		}

		// build Topology the Storm way
		final FlinkTopologyBuilder builder = ExclamationTopology.buildTopology();

		// execute program on Flink cluster
		final Config conf = new Config();
		// We can set Jobmanager host/port values manually or leave them blank
		// if not set and
		// - executed within Java, default values "localhost" and "6123" are set by FlinkSubmitter
		// - executed via bin/flink values from flink-conf.yaml are set by FlinkSubmitter.
		// conf.put(Config.NIMBUS_HOST, "localhost");
		// conf.put(Config.NIMBUS_THRIFT_PORT, new Integer(6123));

		// The user jar file must be specified via JVM argument if executed via Java.
		// => -Dstorm.jar=target/flink-storm-examples-0.9-SNAPSHOT-WordCountStorm.jar
		// If bin/flink is used, the jar file is detected automatically.
		FlinkSubmitter.submitTopology(topologyId, conf, builder.createTopology());

		Thread.sleep(5 * 1000);

		FlinkClient.getConfiguredClient(conf).killTopology(topologyId);
	}
}
