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

package eu.stratosphere.nephele.taskmanager.transferenvelope;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.Queue;

import eu.stratosphere.nephele.event.task.EventList;
import eu.stratosphere.nephele.io.AbstractID;
import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.BufferFactory;
import eu.stratosphere.nephele.io.channels.ChannelID;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;

final class SpillingQueueElement {

	private static final int SIZE_LIMIT = 16;

	private static final Object NULL_OBJECT = new Object();

	private final JobID jobID;

	private final ChannelID source;

	private int tailSequenceNumber = -1;

	private int headSequenceNumber = -1;

	private Object bufferRef = null;

	private EventList eventList = null;

	private SpillingQueueElement nextElement = null;

	private final class SpillingQueueElementIterator implements Iterator<TransferEnvelope> {

		private int headSequenceNumber;

		private final int tailSequenceNumber;

		private final Iterator<Object> bufferIterator;

		private final EventList eventList;

		private SpillingQueueElementIterator(final int headSequenceNumber, final int tailSequenceNumber,
				final Iterator<Object> bufferIterator, final EventList eventList) {

			this.headSequenceNumber = headSequenceNumber;
			this.tailSequenceNumber = tailSequenceNumber;
			this.bufferIterator = bufferIterator;
			this.eventList = eventList;
		}

		@Override
		public boolean hasNext() {

			if (this.headSequenceNumber <= this.tailSequenceNumber) {
				return true;
			}

			return false;
		}

		@Override
		public TransferEnvelope next() {

			Buffer buffer = null;
			final Object obj = this.bufferIterator.next();
			if (obj != NULL_OBJECT) {
				buffer = (Buffer) obj;
			}

			final TransferEnvelope te = new TransferEnvelope(this.headSequenceNumber, jobID, source, this.eventList);
			te.setBuffer(buffer);

			++this.headSequenceNumber;

			return te;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("The remove operation is not supported by this type of iterator");

		}

	}

	SpillingQueueElement(final TransferEnvelope transferEnvelope) {

		if (transferEnvelope == null) {
			throw new IllegalArgumentException("Argument transferEnvelope must not be null");
		}

		this.jobID = transferEnvelope.getJobID();
		this.source = transferEnvelope.getSource();

		add(transferEnvelope);
	}

	boolean canBeAdded(final TransferEnvelope transferEnvelope) {

		if (this.tailSequenceNumber == -1) {
			return true;
		}

		final EventList eventList = transferEnvelope.getEventList();
		if (eventList != null) {
			if (!eventList.isEmpty()) {
				return false;
			}
		}

		if (!this.jobID.equals(transferEnvelope.getJobID())) {
			return false;
		}

		if (!this.source.equals(transferEnvelope.getSource())) {
			return false;
		}

		if (this.tailSequenceNumber != (transferEnvelope.getSequenceNumber() - 1)) {
			return false;
		}

		if (this.size() >= SIZE_LIMIT) {
			return false;
		}

		return true;
	}

	void add(final TransferEnvelope transferEnvelope) {

		if (!canBeAdded(transferEnvelope)) {
			throw new IllegalStateException("Cannot add transfer envelope to this spilling queue element");
		}

		if (this.tailSequenceNumber == -1) {
			this.tailSequenceNumber = transferEnvelope.getSequenceNumber();
			this.headSequenceNumber = this.tailSequenceNumber;
			this.bufferRef = transferEnvelope.getBuffer();
			this.eventList = transferEnvelope.getEventList();
		} else {
			this.tailSequenceNumber = transferEnvelope.getSequenceNumber();
			if (this.tailSequenceNumber == (this.headSequenceNumber + 1)) {
				final Buffer buf = (Buffer) this.bufferRef;
				final Queue<Object> bufferQueue = new CapacityConstrainedArrayQueue<Object>(SIZE_LIMIT);
				if (buf == null) {
					bufferQueue.offer(NULL_OBJECT);
				} else {
					bufferQueue.offer(buf);
				}

				this.bufferRef = bufferQueue;
			}

			@SuppressWarnings("unchecked")
			final Queue<Object> bufferQueue = (Queue<Object>) this.bufferRef;
			final Buffer buf = transferEnvelope.getBuffer();
			if (buf == null) {
				bufferQueue.offer(NULL_OBJECT);
			} else {
				bufferQueue.offer(buf);
			}
		}
	}

	TransferEnvelope peek() {

		if (this.headSequenceNumber == -1) {
			return null;
		}

		Buffer buffer = null;

		if (this.headSequenceNumber == this.tailSequenceNumber) {
			buffer = (Buffer) this.bufferRef;
		} else {
			@SuppressWarnings("unchecked")
			final Queue<Object> bufferQueue = (Queue<Object>) this.bufferRef;
			final Object obj = bufferQueue.peek();
			if (obj == NULL_OBJECT) {
				buffer = null;
			} else {
				buffer = (Buffer) obj;
			}
		}

		// Reconstruct envelope
		final TransferEnvelope transferEnvelope = new TransferEnvelope(this.headSequenceNumber, this.jobID,
			this.source, this.eventList);
		transferEnvelope.setBuffer(buffer);

		return transferEnvelope;
	}

	TransferEnvelope poll() {

		if (this.headSequenceNumber == -1) {
			return null;
		}

		Buffer buffer = null;

		if (this.headSequenceNumber == this.tailSequenceNumber) {
			buffer = (Buffer) this.bufferRef;
		} else {
			@SuppressWarnings("unchecked")
			final Queue<Object> bufferQueue = (Queue<Object>) this.bufferRef;
			final Object obj = bufferQueue.poll();
			if (obj == NULL_OBJECT) {
				buffer = null;
			} else {
				buffer = (Buffer) obj;
			}
		}

		// Reconstruct envelope
		final TransferEnvelope transferEnvelope = new TransferEnvelope(this.headSequenceNumber, this.jobID,
			this.source, this.eventList);
		transferEnvelope.setBuffer(buffer);

		// Maintain consistency of the data structures
		++this.headSequenceNumber;

		if (this.headSequenceNumber == this.tailSequenceNumber) {
			@SuppressWarnings("unchecked")
			final Queue<Object> bufferQueue = (Queue<Object>) this.bufferRef;
			final Object obj = bufferQueue.poll();
			if (obj == NULL_OBJECT) {
				buffer = null;
			} else {
				buffer = (Buffer) obj;
			}

			this.bufferRef = buffer;

		} else if (this.headSequenceNumber > this.tailSequenceNumber) {
			// No more elements
			this.headSequenceNumber = -1;
			this.tailSequenceNumber = -1;
		}

		return transferEnvelope;
	}

	int size() {

		if (this.headSequenceNumber == -1) {
			return 0;
		}

		return (this.tailSequenceNumber - this.headSequenceNumber + 1);
	}

	void clear() {

		if (this.headSequenceNumber == -1) {
			// Element is already cleared
			return;
		}

		if (this.headSequenceNumber == this.tailSequenceNumber) {
			Object obj = this.bufferRef;
			if (obj != NULL_OBJECT) {
				final Buffer buf = (Buffer) obj;
				buf.recycleBuffer();
			}
			this.bufferRef = null;
		} else {
			@SuppressWarnings("unchecked")
			final Queue<Object> bufferQueue = (Queue<Object>) this.bufferRef;
			Object obj = bufferQueue.poll();
			while (obj != null) {

				if (obj != NULL_OBJECT) {
					final Buffer buf = (Buffer) obj;
					buf.recycleBuffer();
				}

				obj = bufferQueue.poll();
			}
		}

		this.eventList = null;

		this.headSequenceNumber = -1;
		this.tailSequenceNumber = -1;
	}

	@SuppressWarnings("unchecked")
	Iterator<TransferEnvelope> iterator() {

		Queue<Object> bufferQueue;

		if (this.headSequenceNumber == -1) {

			bufferQueue = new ArrayDeque<Object>();

		} else if (this.headSequenceNumber == this.tailSequenceNumber) {

			bufferQueue = new ArrayDeque<Object>();

			if (this.bufferRef == null) {
				bufferQueue.offer(NULL_OBJECT);
			} else {
				bufferQueue.offer(this.bufferRef);
			}

		} else {

			bufferQueue = (Queue<Object>) this.bufferRef;
		}

		return new SpillingQueueElementIterator(this.headSequenceNumber, this.tailSequenceNumber,
			bufferQueue.iterator(), this.eventList);
	}

	void setNextElement(final SpillingQueueElement nextElement) {
		this.nextElement = nextElement;
	}

	SpillingQueueElement getNextElement() {
		return this.nextElement;
	}

	int spill(final AbstractID ownerID, final FileBufferManager fileBufferManager) throws IOException {

		if (this.headSequenceNumber == -1) {
			return 0;
		}

		if (this.headSequenceNumber == this.tailSequenceNumber) {

			final Buffer buffer = (Buffer) this.bufferRef;
			if (buffer == null) {
				return 0;
			}

			if (!buffer.isBackedByMemory()) {
				return 0;
			}

			final int size = buffer.size();
			final Buffer fileBuffer = BufferFactory.createFromFile(size, ownerID, fileBufferManager);
			buffer.copyToBuffer(fileBuffer);
			this.bufferRef = fileBuffer;
			buffer.recycleBuffer();

			return size;
		}

		@SuppressWarnings("unchecked")
		final Queue<Object> bufferQueue = (Queue<Object>) this.bufferRef;
		final int queueSize = bufferQueue.size();
		int reclaimedMemory = 0;
		int count = 0;

		while (count++ < queueSize) {

			final Object obj = bufferQueue.poll();
			if (obj == NULL_OBJECT) {
				bufferQueue.offer(obj);
				continue;
			}

			final Buffer buffer = (Buffer) obj;
			if (!buffer.isBackedByMemory()) {
				bufferQueue.offer(buffer);
				continue;
			}

			final int size = buffer.size();
			final Buffer fileBuffer = BufferFactory.createFromFile(size, ownerID, fileBufferManager);
			buffer.copyToBuffer(fileBuffer);
			bufferQueue.offer(fileBuffer);
			buffer.recycleBuffer();
			reclaimedMemory += size;
		}

		return reclaimedMemory;
	}

	int unspill(final BufferProvider bufferProvider) throws IOException {

		if (this.headSequenceNumber == -1) {
			return 0;
		}

		if (this.headSequenceNumber == this.tailSequenceNumber) {

			final Buffer buffer = (Buffer) this.bufferRef;
			if (buffer == null) {
				return 0;
			}

			if (buffer.isBackedByMemory()) {
				return 0;
			}

			final int size = buffer.size();
			final Buffer memBuffer = bufferProvider.requestEmptyBuffer(size);
			if (memBuffer == null) {
				return 0;
			}

			buffer.copyToBuffer(memBuffer);
			this.bufferRef = memBuffer;
			buffer.recycleBuffer();

			return size;
		}

		@SuppressWarnings("unchecked")
		final Queue<Object> bufferQueue = (Queue<Object>) this.bufferRef;
		final int queueSize = bufferQueue.size();
		int usedMemory = 0;
		int count = 0;

		while (count++ < queueSize) {

			final Object obj = bufferQueue.poll();
			if (obj == NULL_OBJECT) {
				bufferQueue.offer(obj);
				continue;
			}

			final Buffer buffer = (Buffer) obj;
			if (buffer.isBackedByMemory()) {
				bufferQueue.offer(buffer);
				continue;
			}

			final int size = buffer.size();
			final Buffer memBuffer = bufferProvider.requestEmptyBuffer(size);
			if (memBuffer != null) {
				buffer.copyToBuffer(memBuffer);
				bufferQueue.offer(memBuffer);
				buffer.recycleBuffer();
			} else {
				bufferQueue.offer(buffer);
				continue;
			}

			usedMemory += size;
		}

		return usedMemory;
	}
}
