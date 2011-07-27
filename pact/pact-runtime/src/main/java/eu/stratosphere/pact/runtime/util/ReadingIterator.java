package eu.stratosphere.pact.runtime.util;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface ReadingIterator<E>
{
	public E next(E target);
}
