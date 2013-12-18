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

package eu.stratosphere.nephele.util;

import java.util.Iterator;

/**
 * An auxiliary implementation of an iterator which protects the underlying collection from being modified. As a result,
 * calling the remove method on this iterator will result in an {@link UnsupportedOperationException}.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 * @param <T>
 *        the type of the encapsulated iterator
 */
public final class UnmodifiableIterator<T> implements Iterator<T> {

	/**
	 * The encapsulated iterator.
	 */
	private final Iterator<T> encapsulatedIterator;

	/**
	 * Constructs a new unmodifiable iterator.
	 * 
	 * @param encapsulatedIterator
	 *        the encapsulated iterator
	 */
	public UnmodifiableIterator(final Iterator<T> encapsulatedIterator) {

		this.encapsulatedIterator = encapsulatedIterator;
	}


	@Override
	public boolean hasNext() {

		return this.encapsulatedIterator.hasNext();
	}


	@Override
	public T next() {

		return this.encapsulatedIterator.next();
	}


	@Override
	public void remove() {

		throw new UnsupportedOperationException("Calling the remove method on this iterator is not allowed");
	}

}
