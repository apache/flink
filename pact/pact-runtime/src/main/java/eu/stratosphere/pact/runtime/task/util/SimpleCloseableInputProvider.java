package eu.stratosphere.pact.runtime.task.util;


import java.io.IOException;
import java.util.Iterator;


/**
 * A simple iterator provider that returns a supplied iterator and does nothing when closed.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class SimpleCloseableInputProvider<E> implements CloseableInputProvider<E>
{
	/**
	 * The iterator returned by this class.
	 */
	private final Iterator<E> iterator;
	
	
	/**
	 * Creates a new simple input provider that will return the given iterator.
	 * 
	 * @param iterator The iterator that will be returned.
	 */
	public SimpleCloseableInputProvider(Iterator<E> iterator) {
		this.iterator = iterator;
	}
	
	
	/* (non-Javadoc)
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		// do nothing
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.CloseableInputProvider#getIterator()
	 */
	@Override
	public Iterator<E> getIterator() {
		return this.iterator;
	}

}
