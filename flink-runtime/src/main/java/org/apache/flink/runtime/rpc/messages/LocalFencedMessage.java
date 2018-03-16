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

import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Local {@link FencedMessage} implementation. This message is used when the communication
 * is local and thus does not require its payload to be serializable.
 *
 * @param <F> type of the fencing token
 * @param <P> type of the payload
 */
public class LocalFencedMessage<F extends Serializable, P> implements FencedMessage<F, P> {

	private final F fencingToken;
	private final P payload;

	public LocalFencedMessage(@Nullable F fencingToken, P payload) {
		this.fencingToken = fencingToken;
		this.payload = Preconditions.checkNotNull(payload);
	}

	@Override
	public F getFencingToken() {
		return fencingToken;
	}

	@Override
	public P getPayload() {
		return payload;
	}

	@Override
	public String toString() {
		return "LocalFencedMessage(" + fencingToken + ", " + payload + ')';
	}
}
