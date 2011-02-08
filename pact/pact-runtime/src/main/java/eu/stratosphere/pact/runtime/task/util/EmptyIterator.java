package eu.stratosphere.pact.runtime.task.util;


import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * An empty iterator that never returns anything.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class EmptyIterator<E> implements Iterator<E> {

	/**
	 * The singleton instance.
	 */
	private static final EmptyIterator<Object> INSTANCE = new EmptyIterator<Object>();
	
	
	/**
	 * Gets a singleton instance of the empty iterator.
	 *  
	 * @param <E> The type of the objects (not) returned by the iterator.
	 * @return An instance of the iterator.
	 */
	public static <E> Iterator<E> get() {
		@SuppressWarnings("unchecked")
		Iterator<E> iter = (Iterator<E>) INSTANCE;
		return iter;
	}
	
	
	
	/**
	 * Always returns false, since this iterator is empty.
	 * 
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return false;
	}

	/**
	 * Always throws a {@link java.util.NoSuchElementException}.
	 *  
	 * @see java.util.Iterator#next()
	 */
	@Override
	public E next() {
		throw new NoSuchElementException();
	}

	/**
	 * Throws a {@link java.lang.UnsupportedOperationException}.
	 * 
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
	
}
