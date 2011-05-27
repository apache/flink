package eu.stratosphere.util;

import java.util.AbstractList;

public class OneElementList<E> extends AbstractList<E> {
	private E element;

	public OneElementList(E element) {
		this.element = element;
	}

	@Override
	public E get(int index) {
		if (index != 0)
			throw new IndexOutOfBoundsException();
		return this.element;
	}
	
	public static <E> OneElementList<E> valueOf(E elem) {
		return new OneElementList<E>(elem);
	}

	@Override
	public int size() {
		return 1;
	}

}
