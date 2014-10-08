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

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.io.network.netty.PartitionRequestClient;
import org.apache.flink.runtime.io.network.partition.IntermediateResultPartitionProvider;

import java.io.IOException;

/**
 * A connection manager implementation to bypass setup overhead for task managers running in local
 * execution mode.
 */
public class LocalConnectionManager implements ConnectionManager {

	@Override
	public void start(IntermediateResultPartitionProvider partitionProvider, TaskEventDispatcher taskEventDispatcher) throws IOException {
	}

	@Override
	public PartitionRequestClient createPartitionRequestClient(RemoteAddress remoteAddress) throws IOException {
		return null;
	}

	@Override
	public int getNumberOfActiveConnections() {
		return 0;
	}

	@Override
	public void shutdown() throws IOException {
	}
}
