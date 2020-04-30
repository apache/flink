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

import org.apache.flink.runtime.rpc.FencedMainThreadExecutable;
import org.apache.flink.util.Preconditions;

/**
 * Wrapper class indicating a message which is not required to match the fencing token
 * as it is used by the {@link FencedMainThreadExecutable} to run code in the main thread without
 * a valid fencing token. This is required for operations which are not scoped by the current
 * fencing token (e.g. leadership grants).
 *
 * <p>IMPORTANT: This message is only intended to be send locally.
 *
 * @param <P> type of the payload
 */
public class UnfencedMessage<P> {
	private final P payload;

	public UnfencedMessage(P payload) {
		this.payload = Preconditions.checkNotNull(payload);
	}

	public P getPayload() {
		return payload;
	}

	@Override
	public String toString() {
		return "UnfencedMessage(" + payload + ')';
	}
}
