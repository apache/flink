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

import org.apache.flink.util.Preconditions;

/**
 * A successful response to a {@link KvStateRequest} containing the serialized
 * result for the requested key and namespace.
 */
public final class KvStateRequestResult {

	/** ID of the request responding to. */
	private final long requestId;

	/**
	 * Serialized result for the requested key and namespace. If no result was
	 * available for the specified key and namespace, this is <code>null</code>.
	 */
	private final byte[] serializedResult;

	/**
	 * Creates a successful {@link KvStateRequestResult} response.
	 *
	 * @param requestId        ID of the request responding to
	 * @param serializedResult Serialized result or <code>null</code> if none
	 */
	KvStateRequestResult(long requestId, byte[] serializedResult) {
		this.requestId = requestId;
		this.serializedResult = Preconditions.checkNotNull(serializedResult, "Serialization result");
	}

	/**
	 * Returns the request ID responding to.
	 *
	 * @return Request ID responding to
	 */
	public long getRequestId() {
		return requestId;
	}

	/**
	 * Returns the serialized result or <code>null</code> if none available.
	 *
	 * @return Serialized result or <code>null</code> if none available.
	 */
	public byte[] getSerializedResult() {
		return serializedResult;
	}

	@Override
	public String toString() {
		return "KvStateRequestResult{" +
				"requestId=" + requestId +
				", serializedResult.length=" + serializedResult.length +
				'}';
	}
}
