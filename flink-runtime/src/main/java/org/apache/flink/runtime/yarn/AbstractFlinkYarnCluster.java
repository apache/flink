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

package org.apache.flink.runtime.yarn;

import java.net.InetSocketAddress;
import java.util.List;

public abstract class AbstractFlinkYarnCluster {

	public abstract InetSocketAddress getJobManagerAddress();

	public abstract String getWebInterfaceURL();

	public abstract void shutdown();

	public abstract boolean hasBeenStopped();

	public abstract FlinkYarnClusterStatus getClusterStatus();

	public abstract boolean hasFailed();

	/**
	 * @return Diagnostics if the Cluster is in "failed" state.
	 */
	public abstract String getDiagnostics();

	public abstract List<String> getNewMessages();
}
