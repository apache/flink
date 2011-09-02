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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.io.channels.ChannelID;

/**
 * This class contains tests for the {@link EphemeralCheckpoint} class.
 * 
 * @author warneke
 */
public class EphemeralCheckpointTest {

	/**
	 * The execution vertex ID which is used during the tests.
	 */
	private static final ExecutionVertexID VERTEX_ID = new ExecutionVertexID();

	/**
	 * The mocked checkpoint manager.
	 */
	@Mock
	private CheckpointManager checkpointManager;

	/**
	 * Initializes the Mockito stubs.
	 */
	@Before
	public void before() {
		MockitoAnnotations.initMocks(this);
	}

	/**
	 * This test checks the initial checkpointing state of an ephemeral checkpoint depending on the channel type.
	 */
	@Test
	public void testInitialCheckpointingState() {

		final EphemeralCheckpoint fileEC = EphemeralCheckpoint.forFileChannel(this.checkpointManager, VERTEX_ID);

		assertTrue(fileEC.isPersistent());
		assertEquals(fileEC.getExecutionVertexID(), VERTEX_ID);

		final EphemeralCheckpoint networkEC = EphemeralCheckpoint.forNetworkChannel(this.checkpointManager, VERTEX_ID);

		assertFalse(networkEC.isPersistent());
		assertEquals(networkEC.getExecutionVertexID(), VERTEX_ID);
	}

	/**
	 * This test checks the registration of output channels for an ephemeral checkpoint.
	 */
	@Test
	public void testOutputChannelRegistration() {

		final EphemeralCheckpoint fileEC = EphemeralCheckpoint.forFileChannel(this.checkpointManager, VERTEX_ID);
		final Set<ChannelID> testChannelIDs = new HashSet<ChannelID>();

		for (int i = 0; i < 10; i++) {
			testChannelIDs.add(new ChannelID());
		}

		final Iterator<ChannelID> it = testChannelIDs.iterator();
		while (it.hasNext()) {
			fileEC.registerOutputChannel(it.next(), null);
		}

		final ChannelID[] registeredOutputChanelIDs = fileEC.getIDsOfCheckpointedOutputChannels();
		assertEquals(registeredOutputChanelIDs.length, testChannelIDs.size());
		for (int i = 0; i < registeredOutputChanelIDs.length; i++) {
			assertTrue(testChannelIDs.contains(registeredOutputChanelIDs[i]));
		}
	}

	/**
	 * Tests the finishing routing of the ephemeral checkpoint.
	 */
	@Test
	public void testFinishingOfCheckpoints() {

		final EphemeralCheckpoint fileEC = EphemeralCheckpoint.forFileChannel(this.checkpointManager, VERTEX_ID);
		final Set<ChannelID> testChannelIDs = new HashSet<ChannelID>();

		for (int i = 0; i < 10; i++) {
			testChannelIDs.add(new ChannelID());
		}

		final Iterator<ChannelID> it = testChannelIDs.iterator();
		while (it.hasNext()) {
			fileEC.registerOutputChannel(it.next(), null);
		}

		assertFalse(fileEC.isFinished());

		final ChannelID[] registeredOutputChanelIDs = fileEC.getIDsOfCheckpointedOutputChannels();
		for (int i = 0; i < 10; i++) {
			fileEC.markChannelAsFinished(registeredOutputChanelIDs[i]);
		}

		assertTrue(fileEC.isFinished());
	}
}
