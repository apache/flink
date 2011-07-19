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

package eu.stratosphere.nephele.io.channels.direct;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.Test;

import eu.stratosphere.nephele.event.task.StringTaskEvent;
import eu.stratosphere.nephele.io.DefaultRecordDeserializer;
import eu.stratosphere.nephele.io.InputGate;
import eu.stratosphere.nephele.io.OutputGate;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.compression.CompressionLevel;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.types.Record;
import eu.stratosphere.nephele.types.StringRecord;

/**
 * This class contains tests to check the functionality of the direct channels, using Gates.
 * 
 * @author casp
 */
public class DirectChannelsTest {

	/**
	 * The job ID used during the tests.
	 */
	private static final JobID JOB_ID = new JobID();

	/**
	 * This test creates an input and output gate and, connects them using direct channels (INMEMORY) and
	 * tests the functioning of record and event transmission and flushing.
	 */
	@Test
	public void testDirectChannels() {

		// create gates
		final InputGate<StringRecord> inGate = new InputGate<StringRecord>(JOB_ID,
			new DefaultRecordDeserializer<StringRecord>(
				StringRecord.class), 1, null);
		final OutputGate<StringRecord> outGate = new OutputGate<StringRecord>(JOB_ID, StringRecord.class, 2, null,
			false);

		// create channels
		final InMemoryInputChannel<StringRecord> in = inGate.createInMemoryInputChannel(new ChannelID(),
			CompressionLevel.NO_COMPRESSION);

		// set buffer size and count (usually being done by TaskManager)
		in.initializeBuffers(4, 100);

		final InMemoryOutputChannel<StringRecord> out = outGate.createInMemoryOutputChannel(new ChannelID(),
			CompressionLevel.NO_COMPRESSION);

		in.setConnectedChannelID(out.getID());
		out.setConnectedChannelID(in.getID());

		final TestDirectChannelBroker broker = new TestDirectChannelBroker(in, out);
		in.setNumberOfConnectionRetries(10);
		out.setNumberOfConnectionRetries(10);

		in.setDirectChannelBroker(broker);
		out.setDirectChannelBroker(broker);

		assertEquals(in.getConnectedChannelID(), out.getID());
		assertEquals(out.getConnectedChannelID(), in.getID());

		try {

			// write and flush records..
			outGate.writeRecord(new StringRecord("test"));
			outGate.flush();

			// is record there?
			StringRecord readrecord = inGate.readRecord();
			assertEquals("test", readrecord.toString());

			// write again after flushing..
			outGate.writeRecord(new StringRecord("test"));
			outGate.flush();

			// is record there?
			readrecord = inGate.readRecord();
			assertEquals("test", readrecord.toString());

			// now write until buffer is full + 5 events
			for (int i = 0; i < 105; i++) {
				outGate.writeRecord(new StringRecord("test"));
			}

			// buffer size is set to 100.. so we should read 100 entries.
			int i = 0;
			while (i < 100 && inGate.readRecord() != null) {
				i++;
			}

			assertEquals(i, 100);

			// request flush of remaining 5 entries in buffer
			outGate.flush();

			i = 0;
			while (i < 5 && inGate.readRecord() != null) {
				i++;
			}

			assertEquals(i, 5);

			inGate.publishEvent(new StringTaskEvent("test"));
			outGate.publishEvent(new StringTaskEvent("test"));

			outGate.requestClose();
			inGate.close();

			assertTrue(outGate.isClosed());
			assertTrue(inGate.isClosed());

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			fail();
		}

	}

	/**
	 * This inner class is used to mock a DirectChannelBroker.
	 * 
	 * @author casp
	 */
	private class TestDirectChannelBroker implements DirectChannelBroker {

		private AbstractDirectInputChannel<? extends Record> in;

		private AbstractDirectOutputChannel<? extends Record> out;

		public TestDirectChannelBroker(AbstractDirectInputChannel<? extends Record> in,
				AbstractDirectOutputChannel<? extends Record> out) {
			super();
			this.in = in;
			this.out = out;
		}

		@Override
		public AbstractDirectInputChannel<? extends Record> getDirectInputChannelByID(ChannelID id) {
			return this.in;
		}

		@Override
		public AbstractDirectOutputChannel<? extends Record> getDirectOutputChannelByID(ChannelID id) {
			return this.out;
		}

	}

}
