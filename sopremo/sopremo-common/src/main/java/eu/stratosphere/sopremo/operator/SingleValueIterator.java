package eu.stratosphere.sopremo.operator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SingleValueIterator<T> implements Iterator<T> {
	private T value;

	private boolean hasNext = true;

	public SingleValueIterator(T value) {
		this.value = value;
	}

	@Override
	public boolean hasNext() {
		return this.hasNext;
	}

	@Override
	public T next() {
		if (!this.hasNext)
			throw new NoSuchElementException();
		this.hasNext = false;
		return this.value;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
