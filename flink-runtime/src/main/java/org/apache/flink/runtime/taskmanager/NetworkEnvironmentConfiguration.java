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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.runtime.io.disk.iomanager.IOManager.IOMode;
import org.apache.flink.runtime.io.network.netty.NettyConfig;

import javax.annotation.Nullable;

/**
 * Configuration object for the network stack.
 */
public class NetworkEnvironmentConfiguration {

	private final float networkBufFraction;

	private final long networkBufMin;

	private final long networkBufMax;

	private final int networkBufferSize;

	private final IOMode ioMode;

	private final int partitionRequestInitialBackoff;

	private final int partitionRequestMaxBackoff;

	private final int networkBuffersPerChannel;

	private final int floatingNetworkBuffersPerGate;

	private final NettyConfig nettyConfig;

	/**
	 * Constructor for a setup with purely local communication (no netty).
	 */
	public NetworkEnvironmentConfiguration(
			float networkBufFraction,
			long networkBufMin,
			long networkBufMax,
			int networkBufferSize,
			IOMode ioMode,
			int partitionRequestInitialBackoff,
			int partitionRequestMaxBackoff,
			int networkBuffersPerChannel,
			int floatingNetworkBuffersPerGate) {

		this(networkBufFraction, networkBufMin, networkBufMax, networkBufferSize,
				ioMode,
				partitionRequestInitialBackoff, partitionRequestMaxBackoff,
				networkBuffersPerChannel, floatingNetworkBuffersPerGate,
				null);
		
	}

	public NetworkEnvironmentConfiguration(
			float networkBufFraction,
			long networkBufMin,
			long networkBufMax,
			int networkBufferSize,
			IOMode ioMode,
			int partitionRequestInitialBackoff,
			int partitionRequestMaxBackoff,
			int networkBuffersPerChannel,
			int floatingNetworkBuffersPerGate,
			@Nullable NettyConfig nettyConfig) {

		this.networkBufFraction = networkBufFraction;
		this.networkBufMin = networkBufMin;
		this.networkBufMax = networkBufMax;
		this.networkBufferSize = networkBufferSize;
		this.ioMode = ioMode;
		this.partitionRequestInitialBackoff = partitionRequestInitialBackoff;
		this.partitionRequestMaxBackoff = partitionRequestMaxBackoff;
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		this.nettyConfig = nettyConfig;
	}

	// ------------------------------------------------------------------------

	public float networkBufFraction() {
		return networkBufFraction;
	}

	public long networkBufMin() {
		return networkBufMin;
	}

	public long networkBufMax() {
		return networkBufMax;
	}

	public int networkBufferSize() {
		return networkBufferSize;
	}

	public IOMode ioMode() {
		return ioMode;
	}

	public int partitionRequestInitialBackoff() {
		return partitionRequestInitialBackoff;
	}

	public int partitionRequestMaxBackoff() {
		return partitionRequestMaxBackoff;
	}

	public int networkBuffersPerChannel() {
		return networkBuffersPerChannel;
	}

	public int floatingNetworkBuffersPerGate() {
		return floatingNetworkBuffersPerGate;
	}

	public NettyConfig nettyConfig() {
		return nettyConfig;
	}

	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		int result = 1;
		result = 31 * result + networkBufferSize;
		result = 31 * result + ioMode.hashCode();
		result = 31 * result + partitionRequestInitialBackoff;
		result = 31 * result + partitionRequestMaxBackoff;
		result = 31 * result + networkBuffersPerChannel;
		result = 31 * result + floatingNetworkBuffersPerGate;
		result = 31 * result + (nettyConfig != null ? nettyConfig.hashCode() : 0);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		else if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		else {
			final NetworkEnvironmentConfiguration that = (NetworkEnvironmentConfiguration) obj;

			return this.networkBufFraction == that.networkBufFraction &&
					this.networkBufMin == that.networkBufMin &&
					this.networkBufMax == that.networkBufMax &&
					this.networkBufferSize == that.networkBufferSize &&
					this.partitionRequestInitialBackoff == that.partitionRequestInitialBackoff &&
					this.partitionRequestMaxBackoff == that.partitionRequestMaxBackoff &&
					this.networkBuffersPerChannel == that.networkBuffersPerChannel &&
					this.floatingNetworkBuffersPerGate == that.floatingNetworkBuffersPerGate &&
					this.ioMode == that.ioMode && 
					(nettyConfig != null ? nettyConfig.equals(that.nettyConfig) : that.nettyConfig == null);
		}
	}

	@Override
	public String toString() {
		return "NetworkEnvironmentConfiguration{" +
				"networkBufFraction=" + networkBufFraction +
				", networkBufMin=" + networkBufMin +
				", networkBufMax=" + networkBufMax +
				", networkBufferSize=" + networkBufferSize +
				", ioMode=" + ioMode +
				", partitionRequestInitialBackoff=" + partitionRequestInitialBackoff +
				", partitionRequestMaxBackoff=" + partitionRequestMaxBackoff +
				", networkBuffersPerChannel=" + networkBuffersPerChannel +
				", floatingNetworkBuffersPerGate=" + floatingNetworkBuffersPerGate +
				", nettyConfig=" + nettyConfig +
				'}';
	}
}
