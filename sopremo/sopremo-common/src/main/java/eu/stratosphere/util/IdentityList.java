package eu.stratosphere.util;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

/**
 * Provides a {@link List} that uses reference-equality and thus intentionally violates the general List contract.
 * 
 * @author Arvid Heise
 * @param <E>
 *        the type of the elements
 * @see List
 * @see IdentityHashMap
 */
public class IdentityList<E> extends AbstractList<E> {
	private List<E> backing = new ArrayList<E>();

	@Override
	public boolean add(E e) {
		return this.backing.add(e);
	}

	@Override
	public void add(int index, E element) {
		this.backing.add(index, element);
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		return this.backing.addAll(c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		return this.backing.addAll(index, c);
	}

	@Override
	public void clear() {
		this.backing.clear();
	}

	@Override
	public boolean contains(Object o) {
		Iterator<E> e = this.iterator();
		while (e.hasNext())
			if (e.next() == o)
				return true;
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		Iterator<?> e = c.iterator();
		while (e.hasNext())
			if (!this.contains(e.next()))
				return false;
		return true;
	}

	@Override
	public boolean equals(Object o) {
		if (o == this)
			return true;
		if (!(o instanceof IdentityList<?>))
			return false;

		ListIterator<E> e1 = this.listIterator();
		@SuppressWarnings("rawtypes")
		ListIterator e2 = ((List) o).listIterator();
		while (e1.hasNext() && e2.hasNext()) {
			E o1 = e1.next();
			Object o2 = e2.next();
			if (o1 != o2)
				return false;
		}
		return !(e1.hasNext() || e2.hasNext());
	}

	@Override
	public E get(int index) {
		return this.backing.get(index);
	}

	@Override
	public int indexOf(Object o) {
		ListIterator<E> e = this.listIterator();
		while (e.hasNext())
			if (e.next() == o)
				return e.previousIndex();
		return -1;
	}

	@Override
	public boolean isEmpty() {
		return this.backing.isEmpty();
	}

	@Override
	public Iterator<E> iterator() {
		return this.backing.iterator();
	}

	@Override
	public int lastIndexOf(Object o) {
		ListIterator<E> e = this.listIterator(this.size());
		while (e.hasPrevious())
			if (e.previous() == o)
				return e.nextIndex();
		return -1;
	}

	@Override
	public ListIterator<E> listIterator() {
		return this.backing.listIterator();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		return this.backing.listIterator(index);
	}

	@Override
	public E remove(int index) {
		return this.backing.remove(index);
	}

	@Override
	public boolean remove(Object o) {
		ListIterator<E> e = this.listIterator();
		while (e.hasNext())
			if (e.next() == o) {
				e.remove();
				return true;
			}
		return false;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean modified = false;
		for (Object object : c)
			modified |= this.remove(object);
		return modified;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		boolean modified = false;
		Iterator<E> e = this.iterator();
		findUnmatchedElement: while (e.hasNext()) {
			E element = e.next();
			Iterator<?> otherIterator = c.iterator();
			while (otherIterator.hasNext())
				if (element == otherIterator.next())
					continue findUnmatchedElement;
			e.remove();
			modified = true;
		}
		return modified;
	}

	@Override
	public E set(int index, E element) {
		return this.backing.set(index, element);
	}

	@Override
	public int size() {
		return this.backing.size();
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		return this.backing.subList(fromIndex, toIndex);
	}

	@Override
	public Object[] toArray() {
		return this.backing.toArray();
	}

	@Override
	public <T> T[] toArray(T[] a) {
		return this.backing.toArray(a);
	}

}
