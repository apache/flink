package eu.stratosphere.nephele.taskmanager.bytebuffered;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Queue;
import java.util.Set;

import eu.stratosphere.nephele.taskmanager.transferenvelope.SpillingQueue;
import eu.stratosphere.nephele.taskmanager.transferenvelope.TransferEnvelope;

public final class TransferEnvelopeQueue implements Queue<TransferEnvelope> {

	private final Queue<TransferEnvelope> defaultQueue = new ArrayDeque<TransferEnvelope>();

	/**
	 * Map to the individual queues for each source channel ID.
	 */
	private final Set<Queue<TransferEnvelope>> channelQueues = new LinkedHashSet<Queue<TransferEnvelope>>();

	private Iterator<Queue<TransferEnvelope>> iterator = null;

	private Queue<TransferEnvelope> headQueue = null;

	private TransferEnvelope peekCache = null;

	@Override
	public boolean addAll(Collection<? extends TransferEnvelope> arg0) {

		throw new UnsupportedOperationException("Method addAll is not supported");
	}

	@Override
	public void clear() {

		this.channelQueues.clear();
		this.headQueue = null;
		this.iterator = null;
	}

	@Override
	public boolean contains(Object arg0) {

		throw new UnsupportedOperationException("Method contains is not supported");
	}

	@Override
	public boolean containsAll(Collection<?> arg0) {

		throw new UnsupportedOperationException("Method containsAll is not supported");
	}

	@Override
	public boolean isEmpty() {

		return (this.size() == 0);
	}

	@Override
	public Iterator<TransferEnvelope> iterator() {

		throw new UnsupportedOperationException("Method iterator is not supported");
	}

	@Override
	public boolean remove(Object arg0) {

		throw new UnsupportedOperationException("Method remove is not supported");
	}

	@Override
	public boolean removeAll(Collection<?> arg0) {

		throw new UnsupportedOperationException("Method removeAll is not supported");
	}

	@Override
	public boolean retainAll(Collection<?> arg0) {

		throw new UnsupportedOperationException("Method retainAll is not supported");
	}

	@Override
	public int size() {

		int size = 0;
		final Iterator<Queue<TransferEnvelope>> it = this.channelQueues.iterator();
		while (it.hasNext()) {
			size += it.next().size();
		}

		return size;
	}

	@Override
	public Object[] toArray() {

		throw new UnsupportedOperationException("Method toArray is not supported");
	}

	@Override
	public <T> T[] toArray(T[] arg0) {

		throw new UnsupportedOperationException("Method toArray is not supported");
	}

	@Override
	public boolean add(final TransferEnvelope e) {

		boolean retVal = this.defaultQueue.add(e);
		if (this.channelQueues.add(this.defaultQueue)) {
			this.iterator = null;
		}

		return retVal;
	}

	@Override
	public TransferEnvelope element() {

		throw new UnsupportedOperationException("Method element is not supported");
	}

	@Override
	public boolean offer(TransferEnvelope e) {

		throw new UnsupportedOperationException("Method offer is not supported");
	}

	@Override
	public TransferEnvelope peek() {

		if (this.peekCache == null) {
			this.peekCache = pollInternal();
		}

		return this.peekCache;
	}

	private TransferEnvelope pollInternal() {

		if (isEmpty()) {
			return null;
		}

		checkHeadQueue();

		final TransferEnvelope retVal = this.headQueue.poll();
		if (retVal == null) {
			throw new IllegalStateException("headQueue returned null");
		}

		if (this.headQueue.isEmpty()) {
			this.channelQueues.remove(this.headQueue);
			this.headQueue = null;
			this.iterator = null;
			return retVal;
		}

		if (this.channelQueues.size() > 1) {
			if (this.iterator.hasNext()) {
				this.headQueue = this.iterator.next();
			} else {
				this.iterator = null;
				this.headQueue = null;
			}
		}

		return retVal;

	}

	@Override
	public TransferEnvelope poll() {

		TransferEnvelope retVal;

		if (this.peekCache == null) {
			retVal = this.pollInternal();
		} else {
			retVal = this.peekCache;
			this.peekCache = null;
		}

		return retVal;
	}

	private void checkHeadQueue() {

		if (this.iterator == null) {
			this.iterator = this.channelQueues.iterator();
		}

		if (this.headQueue == null) {

			if (!this.iterator.hasNext()) {
				throw new IllegalStateException("Size is " + size() + " but iterator does not have an element");
			}

			this.headQueue = this.iterator.next();
			if (this.headQueue == null) {
				throw new IllegalStateException("checkHeadQueue turned headQueue into null");
			}
		}
	}

	@Override
	public TransferEnvelope remove() {

		throw new UnsupportedOperationException("Method remove is not supported");
	}

	void registerSpillingQueue(final SpillingQueue spillingQueue) {

		if (this.channelQueues.add(spillingQueue)) {
			this.iterator = null;
		}
	}
}
