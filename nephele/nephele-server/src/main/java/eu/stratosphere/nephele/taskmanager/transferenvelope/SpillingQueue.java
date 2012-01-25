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
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

import eu.stratosphere.nephele.io.AbstractID;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;

public final class SpillingQueue implements Queue<TransferEnvelope> {

	private final FileBufferManager fileBufferManager;

	private final AbstractID ownerID;

	private final BufferProvider bufferProvider;

	private final AtomicInteger size = new AtomicInteger(0);

	private SpillingQueueElement head = null;

	private SpillingQueueElement tail = null;

	private final AtomicInteger sizeOfMemoryBuffers = new AtomicInteger(0);

	private boolean allowAsynchronousUnspilling = true;

	private static final class SpillingQueueID extends AbstractID {
	}

	public SpillingQueue() {
		this(new SpillingQueueID(), null);
	}

	public SpillingQueue(final AbstractID ownerID, final BufferProvider bufferProvider) {

		this.ownerID = ownerID;
		this.fileBufferManager = FileBufferManager.getInstance();
		this.bufferProvider = bufferProvider;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean addAll(final Collection<? extends TransferEnvelope> c) {

		throw new UnsupportedOperationException("addAll is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized void clear() {

		SpillingQueueElement elem = this.head;
		while (elem != null) {

			elem.clear();
			elem = elem.getNextElement();
		}

		this.head = null;
		this.tail = null;
		this.sizeOfMemoryBuffers.set(0);
		this.size.set(0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean contains(final Object o) {

		throw new UnsupportedOperationException("contains is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsAll(final Collection<?> c) {

		throw new UnsupportedOperationException("containsAll is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean isEmpty() {

		return (this.size.get() == 0);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Iterator<TransferEnvelope> iterator() {

		throw new UnsupportedOperationException("iterator is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean remove(final Object o) {

		throw new UnsupportedOperationException("remove is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean removeAll(final Collection<?> c) {

		throw new UnsupportedOperationException("removeAll is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean retainAll(final Collection<?> c) {

		throw new UnsupportedOperationException("retainAll is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int size() {

		return this.size.get();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object[] toArray() {

		throw new UnsupportedOperationException("toArray is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public <T> T[] toArray(T[] a) {

		throw new UnsupportedOperationException("toArray is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized boolean add(final TransferEnvelope transferEnvelope) {

		// First, increase element counter
		final int oldSize = this.size.getAndIncrement();

		if (oldSize == 0) {
			this.head = new SpillingQueueElement(transferEnvelope);
			this.tail = this.head;
		} else {

			synchronized (this.tail) {

				if (this.tail.canBeAdded(transferEnvelope)) {
					this.tail.add(transferEnvelope);
				} else {
					final SpillingQueueElement newTail = new SpillingQueueElement(transferEnvelope);
					this.tail.setNextElement(newTail);
					this.tail = newTail;
				}
			}
		}

		// Keep track of how much main memory is stuck inside this queue
		final Buffer buffer = transferEnvelope.getBuffer();
		if (buffer != null) {
			if (buffer.isBackedByMemory()) {
				this.sizeOfMemoryBuffers.addAndGet(buffer.size());
			}
		}

		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TransferEnvelope element() {

		throw new UnsupportedOperationException("element is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean offer(final TransferEnvelope transferEnvelope) {

		throw new UnsupportedOperationException("offer is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TransferEnvelope peek() {

		throw new UnsupportedOperationException("peek is not supported on this type of queue");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized TransferEnvelope poll() {

		if (isEmpty()) {
			return null;
		}

		TransferEnvelope te;
		SpillingQueueThread unspillThread = null;
		SpillingQueueElement lockedElement = null;
		synchronized (this.head) {

			te = this.head.poll();
			final Buffer buffer = te.getBuffer();
			if (buffer != null) {
				if (!buffer.isBackedByMemory() && this.allowAsynchronousUnspilling) {
					unspillThread = new SpillingQueueThread(this.bufferProvider, this.head, this);
					unspillThread.start();
					lockedElement = this.head;
				}
			}

			if (this.head.size() == 0) {
				this.head = this.head.getNextElement();
			}

			if (this.head == null) {
				this.tail = null;
			}
		}

		// We have triggered the spilling queue thread
		if (unspillThread != null) {
			unspillThread.waitUntilFirstLockIsAcquired();
			// Wait until the spilling queue thread has finished processing this element
			synchronized (lockedElement) {
			}
		}

		// Keep track of how much main memory is stuck inside this queue
		final Buffer buffer = te.getBuffer();
		if (buffer != null) {
			if (buffer.isBackedByMemory()) {
				this.sizeOfMemoryBuffers.addAndGet(-buffer.size());
			}
		}

		// Decrease element counter
		this.size.decrementAndGet();

		return te;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public TransferEnvelope remove() {

		throw new UnsupportedOperationException("remove is not supported on this type of queue");
	}

	private long spill(final boolean includeHead) throws IOException {

		SpillingQueueElement elem = this.head;
		if (!includeHead) {
			if (elem == null) {
				return 0L;
			}

			// Skip the head;
			elem = elem.getNextElement();
		}

		int reclaimedMemory = 0;

		while (elem != null) {

			reclaimedMemory += elem.spill(this.ownerID, this.fileBufferManager);
			elem = elem.getNextElement();
		}

		this.sizeOfMemoryBuffers.addAndGet(-reclaimedMemory);

		return reclaimedMemory;
	}

	public synchronized long spillSynchronouslyIncludingHead() throws IOException {

		return spill(true);
	}

	/**
	 * Prints out the current spilling state of this queue, i.e. how many buffers that are encapsulated inside the
	 * queued transfer envelopes reside in main memory and how many reside on hard disk.
	 */
	public synchronized void printSpillingState() {

		final StringBuilder str = new StringBuilder();
		str.append("Memory footprint of ");
		str.append(this);
		str.append(":\n");
		str.append(size());
		str.append(" elements in queue\n");

		SpillingQueueElement elem = this.head;
		while (elem != null) {

			final Iterator<TransferEnvelope> it = elem.iterator();
			while (it.hasNext()) {
				while (it.hasNext()) {
					final TransferEnvelope te = it.next();
					final Buffer buffer = te.getBuffer();
					if (buffer == null) {
						str.append('X');
					} else {
						if (buffer.isBackedByMemory()) {
							str.append('M');
						} else {
							str.append('F');
						}
					}
				}
			}

			elem = elem.getNextElement();
		}

		str.append('\n');

		System.out.println(str.toString());
	}

	public long getAmountOfMainMemoryInQueue() {

		return this.sizeOfMemoryBuffers.get();
	}

	public void increaseAmountOfMainMemoryInQueue(int amount) {

		this.sizeOfMemoryBuffers.addAndGet(amount);
	}

	public synchronized void disableAsynchronousUnspilling() {
		this.allowAsynchronousUnspilling = false;
	}
}
