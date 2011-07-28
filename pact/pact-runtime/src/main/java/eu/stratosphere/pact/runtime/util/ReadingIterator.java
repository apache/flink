package eu.stratosphere.pact.runtime.util;

import java.io.IOException;

/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface ReadingIterator<E>
{
	public E next(E target) throws IOException;
}
