package eu.stratosphere.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

public abstract class TransformingIterator<From, To> extends AbstractIterator<To> {
	private Iterator<From> input;

	public TransformingIterator(Iterator<From> input) {
		this.input = input;
	}

	@Override
	protected To loadNext() {
		if (!input.hasNext())
			return noMoreElements();
		return transform(input.next());
	}

	protected abstract To transform(From inputObject);
}