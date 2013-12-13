/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.io.channels;

import java.nio.channels.FileChannel;

/**
 * A simple encapsulation of a file channel with an offset. This object is used for purposes, where
 * the channel is accessed by multiple threads and its internal position may be changed.
 */
public class ChannelWithPosition {

	private final FileChannel channel;

	private final long offset;

	ChannelWithPosition(final FileChannel channel, final long offset) {
		this.channel = channel;
		this.offset = offset;
	}

	public FileChannel getChannel() {

		return this.channel;
	}

	public long getOffset() {

		return this.offset;
	}
}
