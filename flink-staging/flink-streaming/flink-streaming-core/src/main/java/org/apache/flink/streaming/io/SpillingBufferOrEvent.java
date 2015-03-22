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

package org.apache.flink.streaming.io;

import java.io.IOException;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;

public class SpillingBufferOrEvent {

	private BufferOrEvent boe;
	private boolean isSpilled = false;

	private SpillReader spillReader;

	private BufferPool bufferPool;

	private int channelIndex;
	private int bufferSize;

	public SpillingBufferOrEvent(BufferOrEvent boe, BufferSpiller spiller, SpillReader reader)
			throws IOException {

		this.boe = boe;
		this.channelIndex = boe.getChannelIndex();
		this.spillReader = reader;

		if (shouldSpill()) {
			spiller.spill(boe.getBuffer());
			isSpilled = true;
			boe = null;
		}
	}

	/**
	 * If the buffer wasn't spilled simply returns the instance from the field,
	 * otherwise reads it from the spill reader
	 */
	public BufferOrEvent getBufferOrEvent() throws IOException {
		if (isSpilled) {
			return new BufferOrEvent(spillReader.readNextBuffer(bufferSize, bufferPool),
					channelIndex);
		} else {
			return boe;
		}
	}

	/**
	 * Checks whether a given buffer should be spilled to disk. Currently it
	 * checks whether the buffer pool from which the buffer was supplied is
	 * empty and only spills if it is. This avoids out of memory exceptions and
	 * also blocks at the input gate.
	 */
	private boolean shouldSpill() throws IOException {
		if (boe.isBuffer()) {
			Buffer buffer = boe.getBuffer();
			this.bufferSize = buffer.getSize();
			BufferRecycler recycler = buffer.getRecycler();

			if (recycler instanceof BufferPool) {
				bufferPool = (BufferPool) recycler;
				Buffer nextBuffer = bufferPool.requestBuffer();
				if (nextBuffer == null) {
					return true;
				} else {
					nextBuffer.recycle();
				}
			}
		}

		return false;
	}

	public boolean isSpilled() {
		return isSpilled;
	}
}
