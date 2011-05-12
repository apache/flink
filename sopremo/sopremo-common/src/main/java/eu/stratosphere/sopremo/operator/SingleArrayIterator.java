package eu.stratosphere.sopremo.operator;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class SingleArrayIterator<T> implements Iterator<T[]> {
	private T[] values;

	private boolean hasNext = true;

	public SingleArrayIterator(T... values) {
		this.values = values;
	}

	@Override
	public boolean hasNext() {
		return this.hasNext;
	}

	@Override
	public T[] next() {
		if (!this.hasNext)
			throw new NoSuchElementException();
		this.hasNext = false;
		return this.values;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
