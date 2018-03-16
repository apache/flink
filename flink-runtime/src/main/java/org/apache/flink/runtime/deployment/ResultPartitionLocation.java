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

import org.apache.flink.runtime.io.network.ConnectionID;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Location of a result partition from the perspective of the consuming task.
 *
 * <p>The location indicates both the instance, on which the partition is produced and the state of
 * the producing task. There are three possibilities:
 *
 * <ol>
 * <li><strong>Local:</strong> The partition is available at the same instance on which the
 * consuming task is (being) deployed and the producing task has registered the result partition.
 *
 * <li><strong>Remote:</strong> The result partition is available at a different instance from the
 * one, on which the consuming task is (being) deployed and the producing task has registered the
 * result partition.
 *
 * <li><strong>Unknown:</strong> The producing task has not yet registered the result partition.
 * When deploying the consuming task, the instance might be known or unknown. In any case, the
 * consuming task cannot request it yet. Instead, it will be updated at runtime after the
 * producing task is guaranteed to have registered the partition. A producing task is guaranteed
 * to have registered the partition after its state has switched to running.
 * </ol>
 */
public class ResultPartitionLocation implements Serializable {

	private static final long serialVersionUID = -6354238166937194463L;
	/** The type of location for the result partition. */
	private final LocationType locationType;

	/** The connection ID of a remote result partition. */
	private final ConnectionID connectionId;

	private enum LocationType {
		LOCAL,
		REMOTE,
		UNKNOWN
	}

	private ResultPartitionLocation(LocationType locationType, ConnectionID connectionId) {
		this.locationType = checkNotNull(locationType);
		this.connectionId = connectionId;
	}

	public static ResultPartitionLocation createRemote(ConnectionID connectionId) {
		return new ResultPartitionLocation(LocationType.REMOTE, checkNotNull(connectionId));
	}

	public static ResultPartitionLocation createLocal() {
		return new ResultPartitionLocation(LocationType.LOCAL, null);
	}

	public static ResultPartitionLocation createUnknown() {
		return new ResultPartitionLocation(LocationType.UNKNOWN, null);
	}

	// ------------------------------------------------------------------------

	public boolean isLocal() {
		return locationType == LocationType.LOCAL;
	}

	public boolean isRemote() {
		return locationType == LocationType.REMOTE;
	}

	public boolean isUnknown() {
		return locationType == LocationType.UNKNOWN;
	}

	public ConnectionID getConnectionId() {
		return connectionId;
	}

	@Override
	public String toString() {
		return "ResultPartitionLocation [" + locationType + (isRemote() ? " [" + connectionId + "]]" : "]");
	}
}
