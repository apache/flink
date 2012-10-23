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
import java.io.InputStream;
import java.net.DatagramPacket;

final class MultiPacketInputStream extends InputStream {

	private final DatagramPacket[] packets;

	private final long creationTime;

	private int nextPacketToRead = 0;

	private byte[] currentBuffer = null;

	private int currentLength = 0;

	private int read = 0;

	MultiPacketInputStream(final short numberOfPackets) {
		this.packets = new DatagramPacket[numberOfPackets];
		this.creationTime = System.currentTimeMillis();
	}

	long getCreationTime() {
		return this.creationTime;
	}

	void addPacket(final short packetNumber, final DatagramPacket datagramPacket) {
		this.packets[packetNumber] = datagramPacket;
	}

	boolean isComplete() {

		for (int i = 0; i < this.packets.length; ++i) {
			if (this.packets[i] == null) {
				return false;
			}
		}

		return true;
	}

	@Override
	public int available() {

		System.out.println("Available called");

		if (!isComplete()) {
			return 0;
		}

		int available = this.currentLength - this.read;
		for (int i = this.nextPacketToRead; i < this.packets.length; ++i) {
			available += this.packets[i].getLength() - RPCMessage.METADATA_SIZE;
		}

		return available;
	}

	@Override
	public void close() {
		// Nothing to do here
	}

	@Override
	public void mark(final int readlimit) {
		// Nothing to do here
	}

	@Override
	public boolean markSupported() {
		return false;
	}

	@Override
	public int read() throws IOException {

		if (!moreDataAvailable()) {
			return -1;
		}

		return this.currentBuffer[this.read++];
	}

	private boolean moreDataAvailable() {

		while (this.read == this.currentLength) {

			if (this.nextPacketToRead == this.packets.length) {
				return false;
			}

			final DatagramPacket dp = this.packets[this.nextPacketToRead++];
			this.currentBuffer = dp.getData();
			this.currentLength = dp.getLength() - RPCMessage.METADATA_SIZE;
			this.read = 0;
		}

		return true;
	}

	@Override
	public int read(final byte[] b) {

		return read(b, 0, b.length);
	}

	@Override
	public int read(final byte[] b, final int off, final int len) {

		if (!moreDataAvailable()) {
			System.out.println("No data available");
			return -1;
		}

		final int r = Math.min(len, this.currentLength - this.read);
		System.arraycopy(this.currentBuffer, this.read, b, off, r);
		this.read += r;

		return r;
	}

	@Override
	public void reset() {
		this.read = 0;
	}

	@Override
	public long skip(long n) {

		if (!moreDataAvailable()) {
			return 0L;
		}

		final int dataLeftInBuffer = this.currentLength - this.read;

		if (n > dataLeftInBuffer) {
			this.read = this.currentLength;
			return dataLeftInBuffer;
		}

		this.read += (int) n;

		return n;
	}

}
