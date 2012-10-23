/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.nephele.rpc;

import java.io.IOException;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;

final class MultiPacketOutputStream extends OutputStream {

	private byte[] buf;

	private int totalLen = 0;

	private int lenInPacket = 0;

	MultiPacketOutputStream(final int initialBufferSize) {
		this.buf = new byte[initialBufferSize];
	}

	@Override
	public void write(final int b) throws IOException {

		if (this.totalLen + RPCMessage.METADATA_SIZE == this.buf.length) {
			resizeBuffer();
		}

		if (this.lenInPacket == RPCMessage.MAXIMUM_MSG_SIZE) {
			this.lenInPacket = 0;
			this.totalLen += RPCMessage.METADATA_SIZE;
		}

		this.buf[this.totalLen++] = (byte) b;
		++this.lenInPacket;
	}

	@Override
	public void write(final byte[] b) throws IOException {

		write(b, 0, b.length);
	}

	private static int getLengthIncludingMetaData(final int length) {

		final int numberOfPackets = ((length + RPCMessage.MAXIMUM_MSG_SIZE - 1) / RPCMessage.MAXIMUM_MSG_SIZE);

		return length + (numberOfPackets * RPCMessage.METADATA_SIZE);
	}

	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException {

		final int lengthIncludingMetaData = getLengthIncludingMetaData(len);
		while (this.totalLen + lengthIncludingMetaData > this.buf.length) {
			resizeBuffer();
		}

		int written = 0;
		while (written < len) {

			if (this.lenInPacket == RPCMessage.MAXIMUM_MSG_SIZE) {
				this.lenInPacket = 0;
				this.totalLen += RPCMessage.METADATA_SIZE;
			}

			final int amountOfDataToWrite = Math.min((len - written), (RPCMessage.MAXIMUM_MSG_SIZE - this.lenInPacket));
			System.arraycopy(b, off + written, this.buf, this.totalLen, amountOfDataToWrite);
			this.lenInPacket += amountOfDataToWrite;
			this.totalLen += amountOfDataToWrite;
			written += amountOfDataToWrite;
		}
	}

	private void resizeBuffer() {

		final byte[] newBuf = new byte[this.buf.length * 2];
		System.arraycopy(this.buf, 0, newBuf, 0, this.totalLen);
		this.buf = newBuf;
	}

	@Override
	public void close() {
		// Nothing to do here
	}

	@Override
	public void flush() {
		// Nothing to do here
	}

	void reset() {
		this.lenInPacket = 0;
		this.totalLen = 0;
	}

	DatagramPacket[] createPackets(final InetSocketAddress remoteAddress) {

		if (this.totalLen == 0) {
			return new DatagramPacket[0];
		}

		// System.out.println("SENT REQUEST ID " + requestID);

		final int maximumPacketSize = RPCMessage.MAXIMUM_MSG_SIZE + RPCMessage.METADATA_SIZE;
		final short numberOfPackets = (short) (this.totalLen / maximumPacketSize + 1);

		short fragmentationID = 0;
		if (numberOfPackets > 1) {
			fragmentationID = (short) ((double) Short.MIN_VALUE + (Math.random() * 2.0 * (double) Short.MAX_VALUE));
		}

		final DatagramPacket[] packets = new DatagramPacket[numberOfPackets];

		// Write meta data
		for (short i = 0; i < numberOfPackets; ++i) {
			final boolean lastPacket = (i == (numberOfPackets - 1));
			int offset;
			if (lastPacket) {
				offset = (numberOfPackets - 1) * maximumPacketSize + this.lenInPacket;
			} else {
				offset = (i + 1) * maximumPacketSize - RPCMessage.METADATA_SIZE;
			}
			RPCService.shortToByteArray(i, this.buf, offset);
			RPCService.shortToByteArray(numberOfPackets, this.buf, offset + 2);
			RPCService.shortToByteArray(fragmentationID, this.buf, offset + 4);

			DatagramPacket packet;
			if (lastPacket) {
				packet = new DatagramPacket(this.buf, i * maximumPacketSize, this.lenInPacket
					+ RPCMessage.METADATA_SIZE);
			} else {
				packet = new DatagramPacket(this.buf, i * maximumPacketSize, maximumPacketSize);
			}

			packet.setSocketAddress(remoteAddress);
			packets[i] = packet;
		}

		return packets;
	}
}
