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

package org.apache.flink.runtime.query;

import org.apache.flink.runtime.query.netty.KvStateServer;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.net.InetAddress;

/**
 * The (host, port)-address of a {@link KvStateServer}.
 */
public class KvStateServerAddress implements Serializable {

	private static final long serialVersionUID = 1L;

	/** KvStateServer host address. */
	private final InetAddress hostAddress;

	/** KvStateServer port. */
	private final int port;

	/**
	 * Creates a KvStateServerAddress for the given KvStateServer host address
	 * and port.
	 *
	 * @param hostAddress KvStateServer host address
	 * @param port        KvStateServer port
	 */
	public KvStateServerAddress(InetAddress hostAddress, int port) {
		this.hostAddress = Preconditions.checkNotNull(hostAddress, "Host address");
		Preconditions.checkArgument(port > 0 && port <= 65535, "Port " + port + " is out of range 1-65535");
		this.port = port;
	}

	/**
	 * Returns the host address of the KvStateServer.
	 *
	 * @return KvStateServer host address
	 */
	public InetAddress getHost() {
		return hostAddress;
	}

	/**
	 * Returns the port of the KvStateServer.
	 *
	 * @return KvStateServer port
	 */
	public int getPort() {
		return port;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		KvStateServerAddress that = (KvStateServerAddress) o;

		return port == that.port && hostAddress.equals(that.hostAddress);
	}

	@Override
	public int hashCode() {
		int result = hostAddress.hashCode();
		result = 31 * result + port;
		return result;
	}
}
