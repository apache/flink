package eu.stratosphere.util;

import java.util.AbstractList;

/**
 * Provides a read-only, type-safe wrapper around one element.
 * 
 * @author Arvid Heise
 * @param <E>
 *        the type of the element
 */
public class OneElementList<E> extends AbstractList<E> {
	private final E element;

	/**
	 * Initializes OneElementList with the given element.
	 * 
	 * @param element
	 *        the element to wrap
	 */
	public OneElementList(final E element) {
		this.element = element;
	}

	@Override
	public E get(final int index) {
		if (index != 0)
			throw new IndexOutOfBoundsException();
		return this.element;
	}

	@Override
	public int size() {
		return 1;
	}

}
