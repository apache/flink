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

package org.apache.flink.queryablestate.messages;

import org.apache.flink.annotation.Internal;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.queryablestate.network.messages.MessageBody;
import org.apache.flink.queryablestate.network.messages.MessageDeserializer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;

/**
 * The request to be forwarded by the {@link org.apache.flink.runtime.query.KvStateClientProxy
 * Queryable State Client Proxy} to the {@link org.apache.flink.runtime.query.KvStateServer State Server}
 * of the Task Manager responsible for the requested state.
 */
@Internal
public class KvStateInternalRequest extends MessageBody {

	private final KvStateID kvStateId;
	private final byte[] serializedKeyAndNamespace;

	public KvStateInternalRequest(
			final KvStateID stateId,
			final byte[] serializedKeyAndNamespace) {

		this.kvStateId = Preconditions.checkNotNull(stateId);
		this.serializedKeyAndNamespace = Preconditions.checkNotNull(serializedKeyAndNamespace);
	}

	public KvStateID getKvStateId() {
		return kvStateId;
	}

	public byte[] getSerializedKeyAndNamespace() {
		return serializedKeyAndNamespace;
	}

	@Override
	public byte[] serialize() {

		// KvStateId + sizeOf(serializedKeyAndNamespace) + serializedKeyAndNamespace
		final int size = KvStateID.SIZE + Integer.BYTES + serializedKeyAndNamespace.length;

		return ByteBuffer.allocate(size)
				.putLong(kvStateId.getLowerPart())
				.putLong(kvStateId.getUpperPart())
				.putInt(serializedKeyAndNamespace.length)
				.put(serializedKeyAndNamespace)
				.array();
	}

	/**
	 * A {@link MessageDeserializer deserializer} for {@link KvStateInternalRequest}.
	 */
	public static class KvStateInternalRequestDeserializer implements MessageDeserializer<KvStateInternalRequest> {

		@Override
		public KvStateInternalRequest deserializeMessage(ByteBuf buf) {
			KvStateID kvStateId = new KvStateID(buf.readLong(), buf.readLong());

			int length = buf.readInt();
			Preconditions.checkArgument(length >= 0,
					"Negative length for key and namespace. " +
							"This indicates a serialization error.");

			byte[] serializedKeyAndNamespace = new byte[length];
			if (length > 0) {
				buf.readBytes(serializedKeyAndNamespace);
			}
			return new KvStateInternalRequest(kvStateId, serializedKeyAndNamespace);
		}
	}
}
