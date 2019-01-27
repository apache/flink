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

package org.apache.flink.runtime.deployment;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.external.ExternalBlockShuffleServiceOptions;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

import java.net.InetSocketAddress;

/**
 * Tracks the location of external result partitions, e.g. the result partitions served by yarn shuffle service.
 */
public class ExternalResultPartitionLocationTracker implements ResultPartitionLocationTracker {
	/**
	 * Flink configuration which has merged shuffle port information from hadoop configuration.
	 */
	private final Configuration configuration;

	public ExternalResultPartitionLocationTracker(Configuration configuration) {
		this.configuration = configuration;
	}

	@Override
	public ResultPartitionLocation getResultPartitionLocation(
		TaskManagerLocation producerLocation,
		TaskManagerLocation consumerLocation,
		IntermediateResult intermediateResult) {
		// use the yarn shuffle service data port
		Integer dataPort = configuration.getInteger(
			ExternalBlockShuffleServiceOptions.FLINK_SHUFFLE_SERVICE_PORT_KEY);

		// use the taskmanager ip address, for the shuffle service deployed on the same host
		// of the taskmanager is used to shuffle data to down streams.
		InetSocketAddress address = new InetSocketAddress(producerLocation.address(), dataPort);
		ConnectionID connectionId = new ConnectionID(address, intermediateResult.getConnectionIndex());

		return ResultPartitionLocation.createRemote(connectionId);
	}
}
