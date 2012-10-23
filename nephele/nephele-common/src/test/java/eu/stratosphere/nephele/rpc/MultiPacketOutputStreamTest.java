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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetSocketAddress;

import org.junit.Test;

import eu.stratosphere.nephele.util.StringUtils;

/**
 * This class contains unit tests for the {@link MultiPacketOutputStream}.
 * 
 * @author warneke
 */
public class MultiPacketOutputStreamTest {

	private InetSocketAddress TEST_REMOTE_ADDRESS = new InetSocketAddress("localhost", 2000);

	/**
	 * Checks the output of the stream for small amounts of data, i.e. the output fits into a single
	 * {@link DatagramPaket}.
	 */
	@Test
	public void testSinglePacketSerialization() {

		final byte[] sourceBuf = new byte[512];
		for (int i = 0; i < sourceBuf.length; ++i) {
			sourceBuf[i] = (byte) (i % 23);
		}

		final MultiPacketOutputStream mpos = new MultiPacketOutputStream(1024);
		try {
			mpos.write(sourceBuf);
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		final DatagramPacket[] packets = mpos.createPackets(TEST_REMOTE_ADDRESS);
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
		assertEquals(0, packetBuf[offset + length - RPCMessage.METADATA_SIZE + 2]);
		assertEquals(1, packetBuf[offset + length - RPCMessage.METADATA_SIZE + 3]);
	}

	/**
	 * Checks the output of the stream for larger amounts of data, i.e. the output is fragmented into several
	 * {@link DatagramPacket} objects.
	 */
	@Test
	public void testMultiPacketSerialization() {

		final byte[] sourceBuf = new byte[1500];
		for (int i = 0; i < sourceBuf.length; ++i) {
			sourceBuf[i] = (byte) (i % 23);
		}

		final MultiPacketOutputStream mpos = new MultiPacketOutputStream(1024);
		try {
			mpos.write(sourceBuf);
		} catch (IOException ioe) {
			fail(StringUtils.stringifyException(ioe));
		}

		final DatagramPacket[] packets = mpos.createPackets(TEST_REMOTE_ADDRESS);
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
		assertEquals(5, lastByte);
		assertEquals(0, packetBuf[offset + length - RPCMessage.METADATA_SIZE]);
		assertEquals(0, packetBuf[offset + length - RPCMessage.METADATA_SIZE + 1]);
		assertEquals(0, packetBuf[offset + length - RPCMessage.METADATA_SIZE + 2]);
		assertEquals(2, packetBuf[offset + length - RPCMessage.METADATA_SIZE + 3]);

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
