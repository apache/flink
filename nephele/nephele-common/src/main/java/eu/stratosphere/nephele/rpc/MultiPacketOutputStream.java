package eu.stratosphere.nephele.rpc;

import java.io.IOException;
import java.io.OutputStream;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;

final class MultiPacketOutputStream extends OutputStream {

	private final int bufferSize;

	private final byte[] buf;

	private int totalLen = 0;

	private int lenInPacket = 0;

	MultiPacketOutputStream(final byte[] buf) {
		this.bufferSize = buf.length - RPCMessage.METADATA_SIZE;
		this.buf = buf;
	}

	@Override
	public void write(final int b) throws IOException {

		if (this.totalLen == this.bufferSize) {
			throw new IOException("Insufficient buffer space");
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

	@Override
	public void write(final byte[] b, final int off, final int len) throws IOException {

		if (this.totalLen + len > this.bufferSize) {
			throw new IOException("Insufficient buffer space");
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
			shortToByteArray(i, this.buf, offset);
			shortToByteArray(numberOfPackets, this.buf, offset + 2);
			shortToByteArray(fragmentationID, this.buf, offset + 4);

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

	private static void shortToByteArray(final short val, final byte[] arr, final int offset) {

		arr[offset] = (byte) ((val & 0xFF00) >> 8);
		arr[offset + 1] = (byte) (val & 0x00FF);
	}
}
