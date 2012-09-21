package eu.stratosphere.nephele.rpc;

import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;

final class MultiPacketInputStream extends InputStream {

	private final DatagramPacket[] packets;

	MultiPacketInputStream(final short numberOfPackets) {
		this.packets = new DatagramPacket[numberOfPackets];
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
		return (this.len - this.read);
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

		if (this.read == this.len) {
			return -1;
		}

		return this.buf[this.read++];
	}

	@Override
	public int read(final byte[] b) {

		return read(b, 0, b.length);
	}

	@Override
	public int read(final byte[] b, final int off, final int len) {

		if (this.read == this.len) {
			return -1;
		}

		final int r = Math.min(len, this.len - this.read);
		System.arraycopy(this.buf, this.read, b, off, r);
		this.read += r;

		return r;
	}

	@Override
	public void reset() {
		this.read = 0;
	}

	@Override
	public long skip(long n) {

		final int dataLeftInBuffer = this.len - this.read;

		if (n > dataLeftInBuffer) {
			this.read = this.len;
			return dataLeftInBuffer;
		}

		this.read += (int) n;

		return n;
	}**/

}
