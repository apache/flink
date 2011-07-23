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

package eu.stratosphere.nephele.taskmanager.bytebuffered;

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.Deque;

import org.junit.Test;

import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelopeSerializer;
import eu.stratosphere.nephele.util.ServerTestUtils;

/**
 * This class contains tests covering the serialization of transfer envelopes to a byte stream.
 * 
 * @author warneke
 */
public class TransferEnvelopeSerializerTest {

	/**
	 * The maximum size of the transfer envelope's buffer.
	 */
	private static final int BUFFER_SIZE = 4096; // 4 KB;

	/**
	 * An arbitrarily chosen byte used to fill the transfer envelope's buffer.
	 */
	private static final byte BUFFER_CONTENT = 13;

	/**
	 * The size of a sequence number.
	 */
	private static final int SIZE_OF_SEQ_NR = 4;

	/**
	 * The size of an ID.
	 */
	private static final int SIZE_OF_ID = 16;

	/**
	 * The size of an integer number.
	 */
	private static final int SIZE_OF_INTEGER = 4;

	/**
	 * The job ID used during the serialization process.
	 */
	private final JobID jobID = new JobID();

	/**
	 * The target channel ID used during the serialization process.
	 */
	private final ChannelID sourceChannelID = new ChannelID();

	/**
	 * Auxiliary class to explicitly access the internal buffer of an ID object.
	 * 
	 * @author warneke
	 */
	private static class SerializationTestID extends AbstractID {

		/**
		 * Constructs a new ID.
		 * 
		 * @param content
		 *        a byte buffer representing the ID
		 */
		private SerializationTestID(byte[] content) {
			super(content);
		}
	}

	/**
	 * This test checks the correctness of the serialization of {@link TransferEnvelope} objects.
	 */
	@Test
	public void testSerialization() {

		try {

			// Generate test file
			final File testFile = generateDataStream();

			// Analyze the test file
			analyzeStream(testFile);

			// Delete the test file
			testFile.delete();

		} catch (IOException e) {
			fail(e.getMessage());
		}
	}

	/**
	 * Generates and serializes a series of {@link TransferEnvelope} objects to a random file.
	 * 
	 * @return the file containing the serializes envelopes
	 * @throws IOException
	 *         thrown if an I/O error occurs while writing the envelopes
	 */
	private File generateDataStream() throws IOException {

		final File outputFile = new File(ServerTestUtils.getTempDir() + File.separator
			+ ServerTestUtils.getRandomFilename());
		final FileOutputStream outputStream = new FileOutputStream(outputFile);
		final FileChannel fileChannel = outputStream.getChannel();
		final Deque<ByteBuffer> recycleQueue = new ArrayDeque<ByteBuffer>();
		final TransferEnvelopeSerializer serializer = new TransferEnvelopeSerializer();
		final ByteBuffer byteBuffer = ByteBuffer.allocate(BUFFER_SIZE);
		final ByteBuffer initBuffer = ByteBuffer.allocate(1);

		// The byte buffer is initialized from this buffer
		initBuffer.put(BUFFER_CONTENT);
		initBuffer.flip();

		// Put byte buffer to recycled queue
		recycleQueue.add(byteBuffer);

		for (int i = 0; i < BUFFER_SIZE; i++) {

			final Buffer buffer = BufferFactory.createFromMemory(i, recycleQueue.poll(), recycleQueue);

			// Initialize buffer
			for (int j = 0; j < i; j++) {
				buffer.write(initBuffer);
				initBuffer.position(0);
			}

			// Finish write phase
			buffer.finishWritePhase();

			final TransferEnvelope transferEnvelope = new TransferEnvelope(i, this.jobID, this.sourceChannelID);
			transferEnvelope.setBuffer(buffer);

			// set envelope to be serialized and write it to file channel
			serializer.setTransferEnvelope(transferEnvelope);
			while (serializer.write(fileChannel))
				;

			// Put buffer back to the recycling queue
			buffer.recycleBuffer();
		}

		fileChannel.close();

		return outputFile;
	}

	/**
	 * Analyzes the given test file and checks whether its content matches Nephele's serialization pattern.
	 * 
	 * @param testFile
	 *        the test file to analyze
	 * @throws IOException
	 *         thrown if an I/O error occurs while reading the test file
	 */
	private void analyzeStream(File testFile) throws IOException {

		FileInputStream fileInputStream = new FileInputStream(testFile);

		for (int i = 0; i < BUFFER_SIZE; i++) {

			readAndCheckSequenceNumber(fileInputStream, i);
			readAndCheckID(fileInputStream, this.jobID);
			readAndCheckID(fileInputStream, this.sourceChannelID);
			readAndCheckNotificationList(fileInputStream);
			readAndCheckBuffer(fileInputStream, i);
		}

		fileInputStream.close();
	}

	/**
	 * Attempts to read a buffer of the given size from the file stream and checks the buffer's content.
	 * 
	 * @param fileInputStream
	 *        the file stream to read from
	 * @param expectedBufferSize
	 *        the expected size of the buffer
	 * @throws IOException
	 *         thrown if an error occurs while reading from the file stream
	 */
	private static void readAndCheckBuffer(FileInputStream fileInputStream, int expectedBufferSize) throws IOException {

		// Check if buffer exists
		assertEquals(1L, fileInputStream.read());

		byte[] temp = new byte[SIZE_OF_INTEGER];
		fileInputStream.read(temp);
		int bufferSize = bufferToInteger(temp);

		assertEquals(expectedBufferSize, bufferSize);

		byte[] buffer = new byte[bufferSize];
		fileInputStream.read(buffer);

		for (int i = 0; i < buffer.length; i++) {
			assertEquals(BUFFER_CONTENT, buffer[i]);
		}
	}

	/**
	 * Attempts to read an empty notification list from the given file input stream.
	 * 
	 * @param fileInputStream
	 *        the file input stream to read from
	 * @throws IOException
	 *         thrown if an I/O occurs while reading data from the stream
	 */
	private void readAndCheckNotificationList(FileInputStream fileInputStream) throws IOException {

		byte[] temp = new byte[SIZE_OF_INTEGER];
		fileInputStream.read(temp);
		final int sizeOfDataBlock = bufferToInteger(temp);

		assertEquals(SIZE_OF_INTEGER, sizeOfDataBlock);

		fileInputStream.read(temp);
		final int sizeOfNotificationList = bufferToInteger(temp);

		assertEquals(0, sizeOfNotificationList);
	}

	/**
	 * Attempts to read an integer number from the given file input stream and compares it to
	 * <code>expectedSequenceNumber</code>.
	 * 
	 * @param fileInputStream
	 *        the file input stream to read from
	 * @param expectedSeqNumber
	 *        the integer number the read number is expected to match
	 * @throws IOException
	 *         thrown if an I/O occurs while reading data from the stream
	 */
	private void readAndCheckSequenceNumber(FileInputStream fileInputStream, int expectedSeqNumber) throws IOException {

		byte[] temp = new byte[SIZE_OF_SEQ_NR];
		fileInputStream.read(temp);
		int seqNumber = bufferToInteger(temp);

		assertEquals(seqNumber, expectedSeqNumber);
	}

	/**
	 * Attempts to read a channel ID from the given file input stream and compares it to <code>expectedChannelID</code>.
	 * 
	 * @param fileInputStream
	 *        the file input stream to read from
	 * @param expectedID
	 *        the ID which the read ID is expected to match
	 * @throws IOException
	 *         thrown if an I/O occurs while reading data from the stream
	 */
	private void readAndCheckID(FileInputStream fileInputStream, AbstractID expectedID) throws IOException {

		byte[] temp = new byte[SIZE_OF_INTEGER];
		fileInputStream.read(temp);

		final int sizeOfID = bufferToInteger(temp); // ID has fixed size and therefore does not announce its size

		assertEquals(sizeOfID, SIZE_OF_ID);

		byte[] id = new byte[sizeOfID];
		fileInputStream.read(id);

		final AbstractID channelID = new SerializationTestID(id);
		assertEquals(expectedID, channelID);
	}

	/**
	 * Converts the first four bytes of the provided buffer's content to an integer number.
	 * 
	 * @param buffer
	 *        the buffer to convert
	 * @return the integer number converted from the first four bytes of the buffer's content
	 */
	private static int bufferToInteger(byte[] buffer) {

		int integer = 0;

		for (int i = 0; i < SIZE_OF_INTEGER; ++i) {
			integer |= (buffer[SIZE_OF_INTEGER - 1 - i] & 0xff) << (i << 3);
		}

		return integer;
	}
}
