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

package eu.stratosphere.runtime.io.network.envelope;

import eu.stratosphere.nephele.AbstractID;
import eu.stratosphere.runtime.io.Buffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class EnvelopeWriter {

	protected static final int MAGIC_NUMBER = 0xBADC0FFE;

	/**
	 * Size of the envelope header: 48 bytes = 4 bytes magic number, 4 bytes sequence number, 16 bytes job id,
	 * 16 bytes sender id, 4 bytes bufferSize, 4 bytes event list length
	 */
	public static final int HEADER_SIZE = 4 + 4 + 2 * AbstractID.SIZE + 4 + 4;

	public static final int MAGIC_NUMBER_OFFSET = 0;

	public static final int SEQUENCE_NUMBER_OFFSET = 4;

	public static final int JOB_ID_OFFSET = 8;

	public static final int CHANNEL_ID_OFFSET = 24;

	public static final int BUFFER_SIZE_OFFSET = 40;

	public static final int EVENTS_SIZE_OFFSET = 44;

	private ByteBuffer currentHeader;

	private ByteBuffer currentEvents;

	private ByteBuffer currentDataBuffer;

	private final ByteBuffer headerBuffer;

	public EnvelopeWriter() {
		this.headerBuffer = ByteBuffer.allocateDirect(HEADER_SIZE);
		this.headerBuffer.order(ByteOrder.LITTLE_ENDIAN);
	}

	/**
	 * @param channel
	 * @return True, if the writer has more pending data for the current envelope, false if not.
	 *
	 * @throws java.io.IOException
	 */
	public boolean writeNextChunk(WritableByteChannel channel) throws IOException {
		// 1) check if the the header is still pending
		if (this.currentHeader != null) {
			channel.write(this.currentHeader);

			if (this.currentHeader.hasRemaining()) {
				// header was not fully written, so we can leave this method
				return true;
			} else {
				this.currentHeader = null;
			}
		}

		// 2) check if there are events pending
		if (this.currentEvents != null) {
			channel.write(this.currentEvents);
			if (this.currentEvents.hasRemaining()) {
				// events were not fully written, so leave this method
				return true;
			} else {
				this.currentEvents = null;
			}
		}

		// 3) write the data buffer
		if (this.currentDataBuffer != null) {
			channel.write(this.currentDataBuffer);
			if (this.currentDataBuffer.hasRemaining()) {
				return true;
			} else {
				this.currentDataBuffer = null;
			}
		}

		return false;
	}

	public void setEnvelopeForWriting(Envelope env) {
		// header
		constructHeader(env);
		this.currentHeader = this.headerBuffer;

		// events (possibly null)
		this.currentEvents = env.getEventsSerialized();

		// data buffer (possibly null)
		Buffer buf = env.getBuffer();
		if (buf != null && buf.size() > 0) {
			this.currentDataBuffer = buf.getMemorySegment().wrap(0, buf.size());
		}
	}

	private void constructHeader(Envelope env) {
		final ByteBuffer buf = this.headerBuffer;

		buf.clear();							// reset
		buf.putInt(MAGIC_NUMBER);
		buf.putInt(env.getSequenceNumber());	// sequence number (4 bytes)
		env.getJobID().write(buf);				// job Id (16 bytes)
		env.getSource().write(buf);				// producerId (16 bytes)

		// buffer size
		buf.putInt(env.getBuffer() == null ? 0 : env.getBuffer().size());

		// size of event list
		buf.putInt(env.getEventsSerialized() == null ? 0 : env.getEventsSerialized().remaining());

		buf.flip();
	}
}
