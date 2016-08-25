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

package org.apache.flink.runtime.query.netty;

import org.apache.flink.runtime.query.netty.message.KvStateRequest;

/**
 * Callback for {@link KvStateClientHandler}.
 */
interface KvStateClientHandlerCallback {

	/**
	 * Called on a successful {@link KvStateRequest}.
	 *
	 * @param requestId       ID of the request
	 * @param serializedValue Serialized value for the request
	 */
	void onRequestResult(long requestId, byte[] serializedValue);

	/**
	 * Called on a failed {@link KvStateRequest}.
	 *
	 * @param requestId ID of the request
	 * @param cause     Cause of the request failure
	 */
	void onRequestFailure(long requestId, Throwable cause);

	/**
	 * Called on any failure, which is not related to a specific request.
	 *
	 * <p>This can be for example a caught Exception in the channel pipeline
	 * or an unexpected channel close.
	 *
	 * @param cause Cause of the failure
	 */
	void onFailure(Throwable cause);

}
