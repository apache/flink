package eu.stratosphere.pact.runtime.util;

import java.util.Iterator;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ReadingIteratorWrapper<E> implements ReadingIterator<E> {
	
	private final Iterator<E> source;
	
	public ReadingIteratorWrapper(Iterator<E> source)
	{
		this.source = source;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.util.ReadingIterator#next(java.lang.Object)
	 */
	@Override
	public E next(E target) {
		if (!this.source.hasNext()) {
			return null;
		}
		return this.source.next();
	}
}
