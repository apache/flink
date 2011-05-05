package eu.stratosphere.sopremo.expressions;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class AbstractIterator<T> implements Iterator<T> {
	private boolean initialized;

	boolean hasNext;

	private T currentValue;

	public AbstractIterator() {
	}

	@Override
	public boolean hasNext() {
		if (!this.initialized)
			this.currentValue = this.loadNext();
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
		if (!this.initialized)
			this.currentValue = this.loadNext();

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