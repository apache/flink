package eu.stratosphere.sopremo;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractIterator<T> implements Iterator<T> {
	private boolean initialized;

	private boolean hasNext = true;

	private T currentValue;

	public AbstractIterator() {
	}

	protected boolean isInitialized() {
		return initialized;
	}
	
	@Override
	public boolean hasNext() {
		if (!this.initialized) {
			this.currentValue = this.loadNext();
			initialized = true;
		}
		return this.hasNext;
	}

	protected T noMoreElements() {
		this.hasNext = false;
		return null;
	}

	@Override
	public T next() {
		if (!this.hasNext)
			throw new NoSuchElementException();
		if (!this.initialized) {
			this.currentValue = this.loadNext();
			initialized = true;
		}

		T value = this.currentValue;
		this.currentValue = this.loadNext();
		return value;
	}

	protected abstract T loadNext();

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}