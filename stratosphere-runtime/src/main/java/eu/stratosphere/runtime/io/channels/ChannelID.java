/***********************************************************************************************************************
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.runtime.io.channels;

import eu.stratosphere.nephele.AbstractID;

import java.nio.ByteBuffer;

public class ChannelID extends AbstractID {

	public ChannelID() {
		super();
	}

	public ChannelID(long lowerPart, long upperPart) {
		super(lowerPart, upperPart);
	}

	public ChannelID(byte[] bytes) {
		super(bytes);
	}

	public static ChannelID fromByteBuffer(ByteBuffer buf) {
		long lower = buf.getLong();
		long upper = buf.getLong();
		return new ChannelID(lower, upper);
	}

	public static ChannelID fromByteBuffer(ByteBuffer buf, int offset) {
		long lower = buf.getLong(offset);
		long upper = buf.getLong(offset + 8);
		return new ChannelID(lower, upper);
	}
}
