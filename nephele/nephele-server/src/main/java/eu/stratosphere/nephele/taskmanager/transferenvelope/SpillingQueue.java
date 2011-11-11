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

import eu.stratosphere.nephele.io.AbstractID;

import eu.stratosphere.nephele.io.channels.Buffer;
import eu.stratosphere.nephele.io.channels.FileBufferManager;
import eu.stratosphere.nephele.taskmanager.bufferprovider.BufferProvider;

public final class SpillingQueue implements Queue<TransferEnvelope> {

	private final FileBufferManager fileBufferManager;

	private final AbstractID ownerID;
	
	private final BufferProvider bufferProvider;

	private int size = 0;

	private SpillingQueueElement head = null;

	private SpillingQueueElement tail = null;

	private TransferEnvelope peekCache = null;

	private long sizeOfMemoryBuffers = 0;

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
		this.sizeOfMemoryBuffers = 0;
		this.size = 0;
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

		return (this.size == 0);
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
	public synchronized int size() {

		return this.size;
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

		if (isEmpty()) {
			this.head = new SpillingQueueElement(transferEnvelope);
			this.tail = this.head;
		} else {

			if (this.tail.canBeAdded(transferEnvelope)) {
				this.tail.add(transferEnvelope);
			} else {
				final SpillingQueueElement newTail = new SpillingQueueElement(transferEnvelope);
				this.tail.setNextElement(newTail);
				this.tail = newTail;
			}
		}

		// Keep track of how much main memory is stuck inside this queue
		final Buffer buffer = transferEnvelope.getBuffer();
		if (buffer != null) {
			if (buffer.isBackedByMemory()) {
				this.sizeOfMemoryBuffers += buffer.size();
			}
		}

		// Increase element counter
		++this.size;

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
	public synchronized TransferEnvelope peek() {

		if (isEmpty()) {
			return null;
		}

		if (this.peekCache == null) {
			this.peekCache = this.head.peek();
		}

		return this.peekCache;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public synchronized TransferEnvelope poll() {

		if (isEmpty()) {
			return null;
		}

		TransferEnvelope te = this.head.poll();
		if (this.head.size() == 0) {
			this.head = this.head.getNextElement();
		}

		if (this.head == null) {
			this.tail = null;
		}

		// Keep track of how much main memory is stuck inside this queue
		final Buffer buffer = te.getBuffer();
		if (buffer != null) {
			if (buffer.isBackedByMemory()) {
				this.sizeOfMemoryBuffers -= buffer.size();
			}
		}

		// If the user has called peek before, return cached element
		if(this.peekCache != null) {
			te = this.peekCache;
			this.peekCache = null;
		}
		
		// Decrease element counter
		--this.size;

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

		this.sizeOfMemoryBuffers -= reclaimedMemory;

		return reclaimedMemory;
	}

	public synchronized long spillSynchronouslyIncludingHead() throws IOException {

		return spill(true);
	}

	public synchronized long getAmountOfMainMemoryInQueue() {

		return this.sizeOfMemoryBuffers;
	}
}
