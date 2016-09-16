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

package org.apache.flink.runtime.query.netty.message;

import org.apache.flink.runtime.query.KvStateID;
import org.apache.flink.runtime.state.KvState;
import org.apache.flink.util.Preconditions;

/**
 * A {@link KvState} instance request for a specific key and namespace.
 */
public final class KvStateRequest {

	/** ID for this request. */
	private final long requestId;

	/** ID of the requested KvState instance. */
	private final KvStateID kvStateId;

	/** Serialized key and namespace to request from the KvState instance. */
	private final byte[] serializedKeyAndNamespace;

	/**
	 * Creates a KvState instance request.
	 *
	 * @param requestId                 ID for this request
	 * @param kvStateId                 ID of the requested KvState instance
	 * @param serializedKeyAndNamespace Serialized key and namespace to request from the KvState
	 *                                  instance
	 */
	KvStateRequest(long requestId, KvStateID kvStateId, byte[] serializedKeyAndNamespace) {
		this.requestId = requestId;
		this.kvStateId = Preconditions.checkNotNull(kvStateId, "KvStateID");
		this.serializedKeyAndNamespace = Preconditions.checkNotNull(serializedKeyAndNamespace, "Serialized key and namespace");
	}

	/**
	 * Returns the request ID.
	 *
	 * @return Request ID
	 */
	public long getRequestId() {
		return requestId;
	}

	/**
	 * Returns the ID of the requested KvState instance.
	 *
	 * @return ID of the requested KvState instance
	 */
	public KvStateID getKvStateId() {
		return kvStateId;
	}

	/**
	 * Returns the serialized key and namespace to request from the KvState
	 * instance.
	 *
	 * @return Serialized key and namespace to request from the KvState instance
	 */
	public byte[] getSerializedKeyAndNamespace() {
		return serializedKeyAndNamespace;
	}

	@Override
	public String toString() {
		return "KvStateRequest{" +
				"requestId=" + requestId +
				", kvStateId=" + kvStateId +
				", serializedKeyAndNamespace.length=" + serializedKeyAndNamespace.length +
				'}';
	}
}
