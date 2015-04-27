/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import java.io.IOException;

import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

public class SpillingBufferOrEvent {

	private BufferOrEvent boe;
	private boolean isSpilled = false;

	private SpillReader spillReader;

	private int channelIndex;
	private int bufferSize;

	public SpillingBufferOrEvent(BufferOrEvent boe, BufferSpiller spiller, SpillReader reader)
			throws IOException {

		this.boe = boe;
		this.channelIndex = boe.getChannelIndex();
		this.spillReader = reader;

		if (boe.isBuffer()) {
			this.bufferSize = boe.getBuffer().getSize();
			spiller.spill(boe.getBuffer());
			this.boe = null;
			this.isSpilled = true;
		}
	}

	/**
	 * If the buffer wasn't spilled simply returns the instance from the field,
	 * otherwise reads it from the spill reader
	 */
	public BufferOrEvent getBufferOrEvent() throws IOException {
		if (isSpilled) {
			boe = new BufferOrEvent(spillReader.readNextBuffer(bufferSize), channelIndex);
			this.isSpilled = false;
			return boe;
		} else {
			return boe;
		}
	}

	public boolean isSpilled() {
		return isSpilled;
	}
}
