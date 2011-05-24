package eu.stratosphere.pact.common.util;


/**
 * A simple iterator interface. The key differences to the {@link java.util.Iterator} are that this
 * iterator accepts an object into which it places the content, and that is consolidates the logic
 * in a single <code>next()</code> function, rather than in two different functions such as
 * <code>hasNext()</code> and <code>next()</code>. 
 *
 * @author Stephan Ewen
 * 
 * @param <E> The element type of the collection iterated over.
 */
public interface MutableObjectIterator<E>
{
	/**
	 * Gets the next element from the collection. The contents of that next element is put into the given target object.
	 * 
	 * @param target The target object into which to place next element. 
	 * @return True, if the target object was properly filled with its contents, false if the iterator is exhausted.
	 */
	public boolean next(E target);
}
