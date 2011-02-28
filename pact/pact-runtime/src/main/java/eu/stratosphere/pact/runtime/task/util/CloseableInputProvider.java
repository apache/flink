package eu.stratosphere.pact.runtime.task.util;


import java.io.Closeable;
import java.util.Iterator;


/**
 * Utility interface for a provider of an input that can be closed.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface CloseableInputProvider<E> extends Closeable
{
	
	/**
	 * Gets the iterator over this input.
	 * 
	 * @return The iterator provided by this iterator provider.
	 * @throws InterruptedException 
	 */
	public Iterator<E> getIterator() throws InterruptedException;
}
