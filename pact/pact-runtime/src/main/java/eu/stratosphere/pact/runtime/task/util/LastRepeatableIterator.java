package eu.stratosphere.pact.runtime.task.util;

import java.util.Iterator;

/**
 * A LastRepeatableIterator allows to repeat the last emitted object again. 
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 * @param <E>
 */
public interface LastRepeatableIterator<E> extends Iterator<E> {

	/**
	 * Return the last returned element again.
	 * 
	 * @return The last returned element.
	 */
	public E repeatLast();
	
}
