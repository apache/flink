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

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import org.junit.Test;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.util.BufferPoolConnector;

public class SpillingQueueElementTest {

	private static final int BUFFER_SIZE = 1024;

	@Test
	public void testSpillingQueueElement() {

		// Create basic data structures for the test
		final ByteBuffer byteBuf1 = ByteBuffer.allocate(BUFFER_SIZE);
		final ByteBuffer byteBuf2 = ByteBuffer.allocate(BUFFER_SIZE);

		final Queue<ByteBuffer> queue = new ArrayDeque<ByteBuffer>();

		final JobID jobID = JobID.generate();
		final ChannelID source = ChannelID.generate();

		final BufferPoolConnector connector = new BufferPoolConnector(queue);

		final Buffer buf1 = BufferFactory.createFromMemory(BUFFER_SIZE, byteBuf1, connector);
		final Buffer buf2 = BufferFactory.createFromMemory(BUFFER_SIZE, byteBuf2, connector);

		final TransferEnvelope te1 = new TransferEnvelope(0, jobID, source);
		te1.setBuffer(buf1);
		final TransferEnvelope te2 = new TransferEnvelope(1, jobID, source);
		final TransferEnvelope te3 = new TransferEnvelope(2, jobID, source);
		te3.setBuffer(buf2);

		// Basic checks
		assertTrue(te1.equals(te1));

		final SpillingQueueElement elem = new SpillingQueueElement(te1);
		assertFalse(elem.canBeAdded(te1));
		assertTrue(elem.canBeAdded(te2));
		assertFalse(elem.canBeAdded(te3));

		// Do some insert and remove operations and check consistency
		elem.add(te2);
		elem.add(te3);
		assertEquals(3, elem.size());
		assertEquals(te1, elem.poll());
		assertEquals(2, elem.size());
		assertEquals(te2, elem.poll());
		assertEquals(1, elem.size());
		assertEquals(te3, elem.poll());
		assertEquals(0, elem.size());
		assertTrue(elem.poll() == null);

		elem.add(te1);
		elem.add(te2);
		assertEquals(te1, elem.poll());
		elem.add(te3);
		assertEquals(te2, elem.poll());
		assertEquals(te3, elem.poll());
		assertEquals(0, elem.size());

		// Test peek
		assertEquals(0, elem.size());
		elem.add(te2);
		elem.add(te3);
		assertEquals(te2, elem.peek());
		assertEquals(2, elem.size());
		assertEquals(te2, elem.poll());
		assertEquals(te3, elem.peek());
		assertEquals(te3, elem.poll());
		assertEquals(0, elem.size());

		// Test iterator
		elem.add(te1);
		elem.add(te2);
		elem.add(te3);
		final Iterator<TransferEnvelope> it = elem.iterator();
		int count = 0;
		while (it.hasNext()) {
			switch (count++) {
			case 0:
				assertEquals(te1, it.next());
				break;
			case 1:
				assertEquals(te2, it.next());
				break;
			case 2:
				assertEquals(te3, it.next());
				break;
			default:
				fail("Iterator moved to forth element which does not exist");
			}
		}

		// Test clear operation
		assertTrue(queue.isEmpty());
		elem.clear();
		assertEquals(2, queue.size());
	}
}
