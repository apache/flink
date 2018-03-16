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

package org.apache.flink.runtime.entrypoint;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Information about the cluster which is shared with the cluster components.
 */
public class ClusterInformation implements Serializable {

	private static final long serialVersionUID = 316958921518479205L;

	private final String blobServerHostname;

	private final int blobServerPort;

	public ClusterInformation(String blobServerHostname, int blobServerPort) {
		this.blobServerHostname = Preconditions.checkNotNull(blobServerHostname);
		Preconditions.checkArgument(
			0 < blobServerPort && blobServerPort < 65_536,
			"The blob port must between 0 and 65_536. However, it was " + blobServerPort + '.');
		this.blobServerPort = blobServerPort;
	}

	public String getBlobServerHostname() {
		return blobServerHostname;
	}

	public int getBlobServerPort() {
		return blobServerPort;
	}

	@Override
	public String toString() {
		return "ClusterInformation{" +
			"blobServerHostname='" + blobServerHostname + '\'' +
			", blobServerPort=" + blobServerPort +
			'}';
	}
}
