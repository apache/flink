package eu.stratosphere.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Concatenates several iterators to one large iterator.<br>
 * In the beginning, all elements of the first iterator are successively returned. If the first iterator is empty, the
 * elements of the second iterator are streamed and so on.
 * 
 * @author Arvid Heise
 * @param <T>
 *        the element type
 */
public final class ConcatenatingIterator<T> extends AbstractIterator<T> {
	private final Deque<Iterator<? extends T>> inputs;

	/**
	 * Initializes a ConcatenatingIterator with an array of iterators. This constructor is not type-safe.
	 * 
	 * @param iterators
	 *        the iterators to concatenate
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ConcatenatingIterator(final Iterator<?>... iterators) {
		this.inputs = new LinkedList<Iterator<? extends T>>((Collection) Arrays.asList(iterators));
	}

	/**
	 * Initializes a type-safe ConcatenatingIterator with a list of iterators.
	 * 
	 * @param iterators
	 *        the iterators to concatenate
	 */
	public ConcatenatingIterator(final List<? extends Iterator<? extends T>> iterators) {
		this.inputs = new LinkedList<Iterator<? extends T>>(iterators);
	}

	@Override
	protected T loadNext() {
		while (!this.inputs.isEmpty()) {
			final Iterator<? extends T> iterator = this.inputs.getFirst();
			if (!iterator.hasNext())
				this.inputs.pop();
			else
				return iterator.next();
		}
		return this.noMoreElements();
	}
}