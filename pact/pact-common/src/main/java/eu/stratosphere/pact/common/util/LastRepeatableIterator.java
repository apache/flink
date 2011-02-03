package eu.stratosphere.pact.common.util;

import java.util.Iterator;

public interface LastRepeatableIterator<E> extends Iterator<E> {

	/**
	 * Return the last returned element again.
	 * 
	 * @return The last returned element.
	 */
	public E repeatLast();
	
}
