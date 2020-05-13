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

package org.apache.flink.runtime.operators.coordination;

import java.util.concurrent.CompletableFuture;

/**
 * Coordinator interface which can handle {@link CoordinationRequest}s
 * and response with {@link CoordinationResponse}s to the client.
 */
public interface CoordinationRequestHandler {

	/**
	 * Called when receiving a request from the client.
	 *
	 * @param request the request received
	 * @return a future containing the response from the coordinator for this request
	 */
	CompletableFuture<CoordinationResponse> handleCoordinationRequest(CoordinationRequest request);
}
