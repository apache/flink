/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.util;

import java.util.Iterator;

/**
 * Base class to wrap an <code>Iterable</code> and manipulate its elements on-the-fly.
 * 
 * @author Arvid Heise
 */
public abstract class WrappingIterable<I, O> extends AbstractIterable<O> {
	private final Iterable<? extends I> originalIterable;

	/**
	 * Initializes WrappingIterable with the given {@link Iterable}.
	 * 
	 * @param originalIterable
	 *        the Iterable to wrap
	 */
	public WrappingIterable(final Iterable<? extends I> originalIterable) {
		this.originalIterable = originalIterable;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<O> iterator() {
		return this.wrap(this.originalIterable.iterator());
	}

	/**
	 * Returns an {@link Iterator} that wraps the iterator of the wrapped {@link Iterable}.
	 * 
	 * @param iterator
	 *        the iterator to wrap
	 * @return the wrapped iterator
	 */
	protected abstract Iterator<O> wrap(Iterator<? extends I> iterator);

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return toString(10);
	}

	/**
	 * @param i
	 * @return
	 */
	public String toString(int numberOfElements) {
		StringBuilder builder = new StringBuilder(getClass().getName()).append(' ');
		appendElements(builder, originalIterable, numberOfElements);
		builder.append(" -> ");
		appendElements(builder, this, numberOfElements);
		return builder.toString();
	}
}
