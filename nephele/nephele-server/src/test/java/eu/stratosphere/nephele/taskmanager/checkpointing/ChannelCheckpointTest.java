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

package eu.stratosphere.nephele.taskmanager.checkpointing;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;

import java.io.EOFException;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.ByteBufferedChannelManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.IncomingConnection;
import eu.stratosphere.nephele.taskmanager.bytebuffered.IncomingConnectionID;
import eu.stratosphere.nephele.taskmanager.bytebuffered.NetworkConnectionManager;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TransferEnvelope;
import eu.stratosphere.nephele.taskmanager.bytebuffered.TransferEnvelopeProcessingLog;
import eu.stratosphere.nephele.util.ServerTestUtils;

/**
 * This class contains tests for the {@link ChannelCheckpoint} class.
 * 
 * @author warneke
 */
public class ChannelCheckpointTest {

	/**
	 * The execution vertex ID used during the tests.
	 */
	private static final ExecutionVertexID VERTEX_ID = new ExecutionVertexID();

	/**
	 * The source channel ID which is expected during these tests.
	 */
	private static final ChannelID SOURCE_CHANNEL_ID = new ChannelID();

	/**
	 * The target channel ID which is expected during these tests.
	 */
	private static final ChannelID TARGET_CHANNEL_ID = new ChannelID();

	/**
	 * The mocked network connection manager
	 */
	@Mock
	private NetworkConnectionManager networkConnectionManager;

	/**
	 * The mocked file buffer manager.
	 */
	@Mock
	private FileBufferManager fileBufferManager;

	/**
	 * The mocked incoming connection object.
	 */
	@Mock
	private IncomingConnection incomingConnection;

	/**
	 * Initializes the Mockito stubs.
	 */
	@Before
	public void before() {
		MockitoAnnotations.initMocks(this);
	}

	/**
	 * This test checks if {@link TransferEnvelope} envelopes are written correctly to the checkpoint.
	 */
	@Test
	public void testWritingToDisk() {

		final String tmpDir = ServerTestUtils.getTempDir();

		final ChannelCheckpoint channelCheckpoint = new ChannelCheckpoint(VERTEX_ID, SOURCE_CHANNEL_ID, tmpDir);
		final TransferEnvelope transferEnvelope = generateTransferEnvelope(SOURCE_CHANNEL_ID, TARGET_CHANNEL_ID, 0);

		try {
			channelCheckpoint.makePersistent();
			channelCheckpoint.addToCheckpoint(transferEnvelope);

			assertFalse(channelCheckpoint.isChannelCheckpointFinished());

			channelCheckpoint.markChannelCheckpointAsFinished();

			assertTrue(channelCheckpoint.isChannelCheckpointFinished());

		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {
			channelCheckpoint.discard();
		}
	}

	/**
	 * This test checks the behavior of the channel checkpoint when a transfer envelope with an unexpected source
	 * channel ID shall be written.
	 */
	@Test
	public void testUnexpectedSourceChannelID() {

		final String tmpDir = ServerTestUtils.getTempDir();
		final ChannelCheckpoint channelCheckpoint = new ChannelCheckpoint(VERTEX_ID, SOURCE_CHANNEL_ID, tmpDir);
		final TransferEnvelope transferEnvelope = generateTransferEnvelope(new ChannelID(), TARGET_CHANNEL_ID, 0);

		try {

			channelCheckpoint.addToCheckpoint(transferEnvelope);

		} catch (IOException ioe) {
			return;
		}

		fail("Channel checkpoint did not detect wrong source channel ID");
	}

	/**
	 * This tests checks the behavior of the channel checkpoint when a transfer envelope with an unexpected sequence
	 * number shall be written.
	 */
	@Test
	public void testUnexpectedSequenceNumber() {

		final String tmpDir = ServerTestUtils.getTempDir();
		final ChannelCheckpoint channelCheckpoint = new ChannelCheckpoint(VERTEX_ID, SOURCE_CHANNEL_ID, tmpDir);
		final TransferEnvelope transferEnvelope = generateTransferEnvelope(SOURCE_CHANNEL_ID, TARGET_CHANNEL_ID, 1);

		try {

			channelCheckpoint.addToCheckpoint(transferEnvelope);

		} catch (IOException ioe) {
			return;
		}

		fail("Channel checkpoint did not detect unexpected sequence number");
	}

	/**
	 * This test checks the recovery mechanism from a channel checkpoint.
	 */
	@Test
	public void testRecovery() {

		final String tmpDir = ServerTestUtils.getTempDir();
		final ChannelCheckpoint channelCheckpoint = new ChannelCheckpoint(VERTEX_ID, SOURCE_CHANNEL_ID, tmpDir);

		// Mock behavior of internal objects
		/*when(this.byteBufferedChannelManager.getFileBufferManager()).thenReturn(this.fileBufferManager);
		when(
			this.byteBufferedChannelManager.registerIncomingConnection(Matchers.any(IncomingConnectionID.class),
				Matchers.any(ReadableByteChannel.class))).thenReturn(this.incomingConnection);*/

		try {
			doThrow(new EOFException()).when(this.incomingConnection).read();
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		}

		try {

			final TransferEnvelope transferEnvelope = generateTransferEnvelope(SOURCE_CHANNEL_ID, TARGET_CHANNEL_ID, 0);
			channelCheckpoint.addToCheckpoint(transferEnvelope);

			channelCheckpoint.makePersistent();
			channelCheckpoint.markChannelCheckpointAsFinished();

			channelCheckpoint.recover(this.networkConnectionManager, this.fileBufferManager);
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} finally {
			channelCheckpoint.discard();
		}
	}

	/**
	 * Creates a dummy transfer envelope which is used during these tests.
	 * 
	 * @param sourceChannelID
	 *        the source channel ID of the envelope
	 * @param targetChannelID
	 *        the target channel ID of the envelope
	 * @param expectedSeqNo
	 *        the sequence number of the envelope
	 * @return the dummy transfer envelope created from the given parameters
	 */
	private static TransferEnvelope generateTransferEnvelope(final ChannelID sourceChannelID,
			final ChannelID targetChannelID, int expectedSeqNo) {

		final TransferEnvelopeProcessingLog processingLog = new TransferEnvelopeProcessingLog(false, true);

		final TransferEnvelope transferEnvelope = new TransferEnvelope(sourceChannelID, targetChannelID, processingLog);
		transferEnvelope.setSequenceNumber(expectedSeqNo);

		return transferEnvelope;
	}
}
