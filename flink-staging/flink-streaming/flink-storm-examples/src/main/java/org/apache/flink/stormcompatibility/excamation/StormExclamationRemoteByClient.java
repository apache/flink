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
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.NotAliveException;
import backtype.storm.utils.Utils;
import org.apache.flink.stormcompatibility.api.FlinkClient;
import org.apache.flink.stormcompatibility.api.FlinkTopologyBuilder;

public class StormExclamationRemoteByClient {

	public final static String topologyId = "Streaming Exclamation";
	private final static String uploadedJarLocation = "target/flink-storm-examples-0.9-SNAPSHOT-ExclamationStorm.jar";

	// *************************************************************************
	// PROGRAM
	// *************************************************************************

	public static void main(final String[] args) throws AlreadyAliveException, InvalidTopologyException,
			NotAliveException {

		if (!ExclamationTopology.parseParameters(args)) {
			return;
		}

		// build Topology the Storm way
		final FlinkTopologyBuilder builder = ExclamationTopology.buildTopology();

		// execute program on Flink cluster
		final Config conf = new Config();
		// can be changed to remote address
		conf.put(Config.NIMBUS_HOST, "localhost");
		// use default flink jobmanger.rpc.port
		conf.put(Config.NIMBUS_THRIFT_PORT, 6123);

		final FlinkClient cluster = FlinkClient.getConfiguredClient(conf);
		cluster.submitTopology(topologyId, uploadedJarLocation, builder.createTopology());

		Utils.sleep(5 * 1000);

		cluster.killTopology(topologyId);
	}
}
