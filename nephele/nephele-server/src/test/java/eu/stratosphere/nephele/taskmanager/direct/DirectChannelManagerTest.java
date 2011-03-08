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

package eu.stratosphere.nephele.taskmanager.direct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.when;

import eu.stratosphere.nephele.event.task.StringTaskEvent;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.RecordDeserializer;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.direct.AbstractDirectInputChannel;
import eu.stratosphere.nephele.io.channels.direct.AbstractDirectOutputChannel;
import eu.stratosphere.nephele.io.channels.direct.InMemoryInputChannel;
import eu.stratosphere.nephele.io.channels.direct.InMemoryOutputChannel;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * This class contains tests for the {@link DirectChannelManager} class.
 * 
 * @author warneke
 */
public class DirectChannelManagerTest {

	/**
	 * The mocked input gate used during the tests.
	 */
	@Mock
	private InputGate<StringRecord> inputGate;

	/**
	 * The mocked output gate used during the tests.
	 */
	@Mock
	private OutputGate<StringRecord> outputGate;

	/**
	 * The mocked object deserializer used during the tests.
	 */
	@Mock
	private RecordDeserializer<StringRecord> deserializer;

	/**
	 * The ID of the input channel used during the tests.
	 */
	private final ChannelID inputChannelID = new ChannelID();

	/**
	 * The ID of the output channel used during the tests.
	 */
	private final ChannelID outputChannelID = new ChannelID();

	/**
	 * Initializes the Mockito stubs.
	 */
	@Before
	public void before() {
		MockitoAnnotations.initMocks(this);
	}

	/**
	 * This test checks the correct registration/deregistration of {@link AbstractDirectInputChannel} and
	 * {@link AbstractDirectOutputChannel} objects.
	 */
	@Test
	public void testDirectChannelManager() {

		final DirectChannelManager dcm = new DirectChannelManager();

		when(this.deserializer.getRecordType()).thenReturn(StringRecord.class);

		final InMemoryInputChannel<StringRecord> inputChannel = new InMemoryInputChannel<StringRecord>(this.inputGate,
			0, this.deserializer, this.inputChannelID, CompressionLevel.NO_COMPRESSION);

		final InMemoryOutputChannel<StringRecord> outputChannel = new InMemoryOutputChannel<StringRecord>(
			this.outputGate, 0, this.outputChannelID, CompressionLevel.NO_COMPRESSION);

		inputChannel.setConnectedChannelID(this.outputChannelID);
		outputChannel.setConnectedChannelID(this.inputChannelID);

		dcm.registerDirectInputChannel(inputChannel);
		dcm.registerDirectOutputChannel(outputChannel);

		// Test if the direct channel manager works as a broker
		try {
			outputChannel.transferEvent(new StringTaskEvent("Test"));
		} catch (IOException ioe) {
			fail(ioe.getMessage());
		} catch (InterruptedException ie) {
			fail(ie.getMessage());
		}

		assertEquals(inputChannel, dcm.getDirectInputChannelByID(this.inputChannelID));
		assertEquals(outputChannel, dcm.getDirectOutputChannelByID(this.outputChannelID));

		dcm.unregisterDirectInputChannel(inputChannel);
		dcm.unregisterDirectOutputChannel(outputChannel);

		assertEquals(null, dcm.getDirectInputChannelByID(this.inputChannelID));
		assertEquals(null, dcm.getDirectOutputChannelByID(this.outputChannelID));
	}
}
