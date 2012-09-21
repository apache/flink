package eu.stratosphere.nephele.rpc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;

import org.junit.Test;

import eu.stratosphere.nephele.util.StringUtils;

public class MultiPacketOutputStreamTest {

	private InetSocketAddress TEST_REMOTE_ADDRESS = new InetSocketAddress("localhost", 2000);

	public void testSinglePacketSerialization() {

		final byte[] sourceBuf = new byte[512];
		for (int i = 0; i < sourceBuf.length; ++i) {
			sourceBuf[i] = (byte) (i % 23);
		}

		final byte[] targetBuf = new byte[8192];
		final MultiPacketOutputStream mpos = new MultiPacketOutputStream(targetBuf);
		try {
			mpos.write(sourceBuf);
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		final DatagramPacket[] packets = mpos.createPackets(TEST_REMOTE_ADDRESS, 0);
		assertNotNull(packets);
		assertEquals(1, packets.length);

		final int offset = packets[0].getOffset();
		final int length = packets[0].getLength();
		assertEquals(0, offset);
		assertEquals(sourceBuf.length + RPCMessage.METADATA_SIZE, length);
		final byte[] packetBuf = packets[0].getData();
		for (int i = offset; i < (offset + length - RPCMessage.METADATA_SIZE); ++i) {
			assertEquals((byte) (i % 23), packetBuf[i]);
		}
		assertEquals(0, packetBuf[offset + length - RPCMessage.METADATA_SIZE]);
		assertEquals(0, packetBuf[offset + length - RPCMessage.METADATA_SIZE + 1]);
		assertEquals(1, packetBuf[offset + length - RPCMessage.METADATA_SIZE + 2]);
		assertEquals(0, packetBuf[offset + length - RPCMessage.METADATA_SIZE + 3]);
	}

	@Test
	public void testMultiPacketSerialization() {

		final byte[] sourceBuf = new byte[1500];
		for (int i = 0; i < sourceBuf.length; ++i) {
			sourceBuf[i] = (byte) (i % 23);
		}

		final byte[] targetBuf = new byte[8192];
		final MultiPacketOutputStream mpos = new MultiPacketOutputStream(targetBuf);
		try {
			mpos.write(sourceBuf);
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		final DatagramPacket[] packets = mpos.createPackets(TEST_REMOTE_ADDRESS, 0);
		assertNotNull(packets);
		assertEquals(2, packets.length);

		// Inspect first packet
		int offset = packets[0].getOffset();
		int length = packets[0].getLength();
		assertEquals(0, offset);
		assertEquals(RPCMessage.MAXIMUM_MSG_SIZE + RPCMessage.METADATA_SIZE, length);
		byte[] packetBuf = packets[0].getData();
		byte lastByte = 0;
		for (int i = offset; i < (offset + length - RPCMessage.METADATA_SIZE); ++i) {
			lastByte = packetBuf[i];
			assertEquals((byte) (i % 23), lastByte);
		}
		assertEquals(7, lastByte);
		System.out.println("Last byte" + lastByte);
		assertEquals(0, packetBuf[offset + length - RPCMessage.METADATA_SIZE]);
		assertEquals(0, packetBuf[offset + length - RPCMessage.METADATA_SIZE + 1]);
		assertEquals(2, packetBuf[offset + length - RPCMessage.METADATA_SIZE + 2]);
		assertEquals(0, packetBuf[offset + length - RPCMessage.METADATA_SIZE + 3]);

		// Inspect second package
		offset = packets[1].getOffset();
		length = packets[1].getLength();
		assertEquals(sourceBuf.length - RPCMessage.MAXIMUM_MSG_SIZE + RPCMessage.METADATA_SIZE, length);
		assertEquals(RPCMessage.MAXIMUM_MSG_SIZE + RPCMessage.METADATA_SIZE, offset);
		packetBuf = packets[1].getData();
		int j = lastByte + 1;
		for (int i = offset; i < (offset + length - RPCMessage.METADATA_SIZE); ++i) {
			assertEquals((byte) (j++ % 23), packetBuf[i]);
		}
	}
}
