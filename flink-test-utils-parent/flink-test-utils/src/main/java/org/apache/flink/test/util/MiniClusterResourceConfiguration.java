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

package org.apache.flink.test.util;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.util.Preconditions;

/**
 * Mini cluster resource configuration object.
 */
public class MiniClusterResourceConfiguration {

	private final Configuration configuration;

	private final int numberTaskManagers;

	private final int numberSlotsPerTaskManager;

	private final Time shutdownTimeout;

	private final TestBaseUtils.CodebaseType codebaseType;

	private final RpcServiceSharing rpcServiceSharing;

	MiniClusterResourceConfiguration(
		Configuration configuration,
		int numberTaskManagers,
		int numberSlotsPerTaskManager,
		Time shutdownTimeout,
		TestBaseUtils.CodebaseType codebaseType,
		RpcServiceSharing rpcServiceSharing) {
		this.configuration = Preconditions.checkNotNull(configuration);
		this.numberTaskManagers = numberTaskManagers;
		this.numberSlotsPerTaskManager = numberSlotsPerTaskManager;
		this.shutdownTimeout = Preconditions.checkNotNull(shutdownTimeout);
		this.codebaseType = Preconditions.checkNotNull(codebaseType);
		this.rpcServiceSharing = Preconditions.checkNotNull(rpcServiceSharing);
	}

	public Configuration getConfiguration() {
		return configuration;
	}

	public int getNumberTaskManagers() {
		return numberTaskManagers;
	}

	public int getNumberSlotsPerTaskManager() {
		return numberSlotsPerTaskManager;
	}

	public Time getShutdownTimeout() {
		return shutdownTimeout;
	}

	/**
	 * @deprecated Will be irrelevant once the legacy mode has been removed.
	 */
	@Deprecated
	public TestBaseUtils.CodebaseType getCodebaseType() {
		return codebaseType;
	}

	public RpcServiceSharing getRpcServiceSharing() {
		return rpcServiceSharing;
	}

	/**
	 * Builder for {@link MiniClusterResourceConfiguration}.
	 */
	public static final class Builder {

		private Configuration configuration = new Configuration();
		private int numberTaskManagers = 1;
		private int numberSlotsPerTaskManager = 1;
		private Time shutdownTimeout = AkkaUtils.getTimeoutAsTime(configuration);
		private TestBaseUtils.CodebaseType codebaseType = TestBaseUtils.getCodebaseType();

		private RpcServiceSharing rpcServiceSharing = RpcServiceSharing.SHARED;

		public Builder setConfiguration(Configuration configuration) {
			this.configuration = configuration;
			return this;
		}

		public Builder setNumberTaskManagers(int numberTaskManagers) {
			this.numberTaskManagers = numberTaskManagers;
			return this;
		}

		public Builder setNumberSlotsPerTaskManager(int numberSlotsPerTaskManager) {
			this.numberSlotsPerTaskManager = numberSlotsPerTaskManager;
			return this;
		}

		public Builder setShutdownTimeout(Time shutdownTimeout) {
			this.shutdownTimeout = shutdownTimeout;
			return this;
		}

		/**
		 * @deprecated Will be irrelevant once the legacy mode has been removed.
		 */
		@Deprecated
		public Builder setCodebaseType(TestBaseUtils.CodebaseType codebaseType) {
			this.codebaseType = codebaseType;
			return this;
		}

		public Builder setRpcServiceSharing(RpcServiceSharing rpcServiceSharing) {
			this.rpcServiceSharing = rpcServiceSharing;
			return this;
		}

		public MiniClusterResourceConfiguration build() {
			return new MiniClusterResourceConfiguration(configuration, numberTaskManagers, numberSlotsPerTaskManager, shutdownTimeout, codebaseType, rpcServiceSharing);
		}
	}
}
