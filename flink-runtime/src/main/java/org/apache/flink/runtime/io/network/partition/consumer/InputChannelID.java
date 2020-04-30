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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.util.AbstractID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/**
 * Identifier for input channels.
 */
public class InputChannelID extends AbstractID {

	private static final long serialVersionUID = 1L;

	public InputChannelID() {
		super();
	}

	public InputChannelID(long lowerPart, long upperPart) {
		super(lowerPart, upperPart);
	}

	public InputChannelID(AbstractID id) {
		super(id);
	}

	public void writeTo(ByteBuf buf) {
		buf.writeLong(this.lowerPart);
		buf.writeLong(this.upperPart);
	}

	public static InputChannelID fromByteBuf(ByteBuf buf) {
		long lower = buf.readLong();
		long upper = buf.readLong();
		return new InputChannelID(lower, upper);
	}
}
