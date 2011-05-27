package eu.stratosphere.util;

import java.util.Iterator;

/**
 * An iterator which takes the elements of another iterator and converts the elements on-the-fly.
 * 
 * @author Arvid Heise
 * @param <From>
 *        the type of the original iterator
 * @param <To>
 *        the return type after the conversion
 */
public abstract class ConversionIterator<From, To> extends AbstractIterator<To> {
	private Iterator<From> iterator;

	/**
	 * Initializes ConversionIterator with the given iterator.
	 * 
	 * @param iterator
	 *        the original iterator to wrap
	 */
	public ConversionIterator(Iterator<From> iterator) {
		this.iterator = iterator;
	}

	@Override
	protected To loadNext() {
		if (!this.iterator.hasNext())
			return this.noMoreElements();
		return this.convert(this.iterator.next());
	}

	/**
	 * Convert the given object to the desired return type.
	 * 
	 * @param inputObject
	 *        the object to convert
	 * @return the result of the conversion of one object
	 */
	protected abstract To convert(From inputObject);
}