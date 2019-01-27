/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph.util;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;

/**
 * A <code>List</code> implementation with a <code>ListIterator</code> that
 * allows concurrent modifications to the underlying list.
 *
 * <p>The main feature of this class is the ability to modify the list and the
 * iterator at the same time.
 *
 * <p>Indexed access is not supported on iterators for performance considerations,
 * and the sub list is also not supported.
 */
public class CursorableLinkedList<E> implements List<E> {

	protected transient Node<E> header;
	protected transient int size;
	protected transient int modCount;

	public CursorableLinkedList() {
		this.header = createHeaderNode();
	}

	public CursorableLinkedList(Collection<? extends E> c) {
		this.header = createHeaderNode();
		addAll(c);
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public boolean isEmpty() {
		return (size() == 0);
	}

	@Override
	public boolean contains(Object o) {
		return indexOf(o) != -1;
	}

	@Override
	public Iterator<E> iterator() {
		return cursor();
	}

	@Override
	public Object[] toArray() {
		return toArray(new Object[size]);
	}

	@Override
	public <T> T[] toArray(T[] a) {
		if (a.length < size) {
			Class componentType = a.getClass().getComponentType();
			a = (T[]) Array.newInstance(componentType, size);
		}

		int i = 0;
		for (Node node = header.next; node != header; node = node.next, i++) {
			a[i] = (T) node.value;
		}

		if (a.length > size) {
			a[size] = null;
		}
		return a;
	}

	@Override
	public boolean add(E e) {
		addNodeBefore(header, e);
		return true;
	}

	@Override
	public boolean remove(Object o) {
		for (Node<E> node = header.next; node != header; node = node.next) {
			if (isEqualValue(node.value, o)) {
				removeNode(node);
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean containsAll(Collection<?> c) {
		Iterator it = c.iterator();
		while (it.hasNext()) {
			if (!contains(it.next())) {
				return false;
			}
		}
		return true;
	}

	@Override
	public boolean addAll(Collection<? extends E> c) {
		return addAll(size, c);
	}

	@Override
	public boolean addAll(int index, Collection<? extends E> c) {
		Node node = getNode(index, true);
		for (Iterator itr = c.iterator(); itr.hasNext();) {
			E value = (E) itr.next();
			addNodeBefore(node, value);
		}
		return true;
	}

	@Override
	public boolean removeAll(Collection<?> c) {
		boolean modified = false;
		Iterator it = iterator();
		while (it.hasNext()) {
			if (c.contains(it.next())) {
				it.remove();
				modified = true;
			}
		}
		return modified;
	}

	@Override
	public boolean retainAll(Collection<?> c) {
		boolean modified = false;
		Iterator it = iterator();
		while (it.hasNext()) {
			if (!c.contains(it.next())) {
				it.remove();
				modified = true;
			}
		}
		return modified;
	}

	@Override
	public void clear() {
		removeAllNodes();
	}

	@Override
	public E get(int index) {
		Node<E> node = getNode(index, false);
		return node.value;
	}

	@Override
	public E set(int index, E element) {
		Node<E> node = getNode(index, false);
		E oldValue = node.value;
		node.value = element;
		return oldValue;
	}

	@Override
	public void add(int index, E element) {
		Node<E> node = getNode(index, true);
		addNodeBefore(node, element);
	}

	@Override
	public E remove(int index) {
		Node<E> node = getNode(index, false);
		E oldValue = node.value;
		removeNode(node);
		return oldValue;
	}

	@Override
	public int indexOf(Object o) {
		int i = 0;
		for (Node node = header.next; node != header; node = node.next) {
			if (isEqualValue(node.value, o)) {
				return i;
			}
			i++;
		}
		return -1;
	}

	@Override
	public int lastIndexOf(Object o) {
		int i = size - 1;
		for (Node node = header.previous; node != header; node = node.previous) {
			if (isEqualValue(node.value, o)) {
				return i;
			}
			i--;
		}
		return -1;
	}

	@Override
	public ListIterator<E> listIterator() {
		return cursor();
	}

	@Override
	public ListIterator<E> listIterator(int index) {
		Node<E> node = getNode(index, true);
		return new Cursor<>(this, node);
	}

	@Override
	public List<E> subList(int fromIndex, int toIndex) {
		throw new UnsupportedOperationException();
	}

	public Cursor<E> cursor() {
		return new Cursor<>(this, header);
	}

	public Cursor<E> cursor(Cursor<E> from) {
		checkCursor(from);
		return new Cursor<>(this, from.current);
	}

	/**
	 * Adds an object to the list.
	 * The object added here will be the new 'previous' in the cursor.
	 *
	 * @param cursor cursor to position the node which to insert before
	 * @param element element to be inserted to this list
	 */
	public void add(Cursor<E> cursor, E element) {
		checkCursor(cursor);
		if (cursor.current.previous == null) {
			throw new IllegalStateException("The element positioned by the cursor was already removed from the list.");
		}

		addNodeBefore(cursor.current, element);
	}

	/**
	 * Removes the specified node from the list.
	 *
	 * @param cursor  the node to remove
	 */
	public void remove(Cursor<E> cursor) {
		checkCursor(cursor);
		if (cursor.current.previous != null) {
			removeNode(cursor.current);
		}
	}

	private Node<E> createHeaderNode() {
		return new Node<>();
	}

	private Node<E> addNodeBefore(Node<E> node, E elment) {
		Node<E> newNode = new Node(node.previous, elment, node);

		node.previous.next = newNode;
		node.previous = newNode;
		size++;
		modCount++;

		return newNode;
	}

	private void addNodeBefore(Node<E> node, Node<E> newNode) {
		newNode.previous = node.previous;
		newNode.next = node;

		node.previous.next = newNode;
		node.previous = newNode;
		size++;
		modCount++;
	}

	private Node<E> removeNode(Node<E> node) {
		if (node != header) {
			node.previous.next = node.next;
			node.next.previous = node.previous;
			size--;
			modCount++;

			node.previous = null;
			node.next = null;

			return node;
		}

		return null;
	}

	private void removeAllNodes() {
		header.next = header;
		header.previous = header;
		size = 0;
		modCount++;
	}

	private Node<E> getNode(int index, boolean endMarkerAllowed) throws IndexOutOfBoundsException {
		// Check the index is within the bounds
		if (index < 0) {
			throw new IndexOutOfBoundsException("Couldn't get the node: " +
					"index (" + index + ") less than zero.");
		}
		if (!endMarkerAllowed && index == size) {
			throw new IndexOutOfBoundsException("Couldn't get the node: " +
					"index (" + index + ") is the size of the list.");
		}
		if (index > size) {
			throw new IndexOutOfBoundsException("Couldn't get the node: " +
					"index (" + index + ") greater than the size of the " +
					"list (" + size + ").");
		}
		// Search the list and get the node
		Node<E> node;
		if (index < (size / 2)) {
			// Search forwards
			node = header.next;
			for (int currentIndex = 0; currentIndex < index; currentIndex++) {
				node = node.next;
			}
		} else {
			// Search backwards
			node = header;
			for (int currentIndex = size; currentIndex > index; currentIndex--) {
				node = node.previous;
			}
		}
		return node;
	}

	private boolean isEqualValue(Object value1, Object value2) {
		return (value1 == value2 || (value1 == null ? false : value1.equals(value2)));
	}

	private void checkCursor(Cursor<E> cursor) {
		if (cursor.parent != this) {
			throw new IllegalArgumentException("The cursor does not point to the list.");
		}
	}

	/**
	 * An extended <code>ListIterator</code> that allows concurrent changes to
	 * the underlying list.
	 */
	public static class Cursor<E> implements ListIterator<E> {

		private CursorableLinkedList<E> parent;
		private Node<E> current;

		protected Cursor(CursorableLinkedList parent, Node<E> node) {
			this.parent = parent;
			this.current = node;
		}

		@Override
		public boolean hasNext() {
			return current.next != parent.header;
		}

		@Override
		public E next() {
			if (!hasNext()) {
				throw new NoSuchElementException("No element at next index.");
			}

			current = current.next;
			return current.value;
		}

		@Override
		public boolean hasPrevious() {
			return current.previous != parent.header;
		}

		@Override
		public E previous() {
			if (!hasPrevious()) {
				throw new NoSuchElementException("No element at previous index.");
			}

			current = current.previous;
			return current.value;
		}

		@Override
		public int nextIndex() {
			throw new UnsupportedOperationException("Indexed access is not supported.");
		}

		@Override
		public int previousIndex() {
			throw new UnsupportedOperationException("Indexed access is not supported.");
		}

		@Override
		public void remove() {
			parent.remove(this);
		}

		@Override
		public void set(E e) {
			current.value = e;
		}

		@Override
		public void add(E e) {
			parent.add(this, e);
		}

		public void moveNodeTo(Cursor<E> cursor) {
			current = parent.removeNode(current);
			parent.addNodeBefore(cursor.current, current);
		}

		public E getValue() {
			if (current == parent.header) {
				throw new NoSuchElementException("No element at current index.");
			}

			return current.value;
		}
	}

	static class Node<E> {
		Node<E> previous;
		Node<E> next;
		E value;

		Node() {
			this.previous = this;
			this.next = this;
		}

		Node(Node<E> previous, E value, Node<E> next) {
			this.previous = previous;
			this.next = next;
			this.value = value;
		}
	}
}
