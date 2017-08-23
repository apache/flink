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

package org.apache.flink.runtime.rpc.messages;

import org.apache.flink.runtime.rpc.FencedRpcEndpoint;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Wrapper class for fenced messages used by the {@link FencedRpcEndpoint}.
 *
 * @param <F> type of the fencing token
 * @param <P> type of the payload
 */
public class FencedMessage<F extends Serializable, P> {
	private final F fencingToken;
	private final P payload;


	public FencedMessage(F fencingToken, P payload) {
		this.fencingToken = Preconditions.checkNotNull(fencingToken);
		this.payload = Preconditions.checkNotNull(payload);
	}

	public F getFencingToken() {
		return fencingToken;
	}

	public P getPayload() {
		return payload;
	}

	@Override
	public String toString() {
		return "FencedMessage(" + fencingToken + ", " + payload + ')';
	}
}
