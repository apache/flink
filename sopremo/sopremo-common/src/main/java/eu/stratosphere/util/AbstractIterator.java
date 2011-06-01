package eu.stratosphere.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Base iterator implementation for a read-only iterator.<br>
 * This skeleton implementation eases the development of new iterators since only {@link #loadNext()} needs to be
 * overwritten.
 * 
 * @author Arvid Heise
 * @param <T>
 *        the type of the elements
 */
public abstract class AbstractIterator<T> implements Iterator<T> {
	private boolean initialized;

	private boolean hasNext = true;

	private T currentValue;

	@Override
	public boolean hasNext() {
		if (!this.initialized) {
			this.currentValue = this.loadNext();
			this.initialized = true;
		}
		return this.hasNext;
	}

	/**
	 * Return true if at least one element has been loaded.
	 * 
	 * @return true if at least one element has been loaded
	 */
	protected boolean isInitialized() {
		return this.initialized;
	}

	/**
	 * Returns the next element or the result of {@link #noMoreElements()}.
	 * 
	 * @return the next element
	 */
	protected abstract T loadNext();

	@Override
	public T next() {
		if (!this.hasNext)
			throw new NoSuchElementException();
		if (!this.initialized) {
			this.currentValue = this.loadNext();
			this.initialized = true;
		}

		T value = this.currentValue;
		this.currentValue = this.loadNext();
		return value;
	}

	/**
	 * Signal methods that should be invoked when no more elements are in the iterator.
	 * 
	 * @return a signal that no more elements are in this iterator
	 */
	protected T noMoreElements() {
		this.hasNext = false;
		return null;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}