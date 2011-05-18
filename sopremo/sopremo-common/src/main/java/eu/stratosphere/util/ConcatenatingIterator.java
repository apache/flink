package eu.stratosphere.util;

import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;

import org.codehaus.jackson.JsonNode;


public final class ConcatenatingIterator<T> extends AbstractIterator<T> {
	private final Deque<Iterator<T>> inputs;

	public ConcatenatingIterator(Iterator<T>... inputs) {
		this.inputs = new LinkedList<Iterator<T>>(Arrays.asList(inputs));
	}

	@Override
	protected T loadNext() {
		while (!this.inputs.isEmpty()) {
			Iterator<T> iterator = this.inputs.getFirst();
			if (!iterator.hasNext())
				this.inputs.pop();
			else
				return iterator.next();
		}
		return this.noMoreElements();
	}
}