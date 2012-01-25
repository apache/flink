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
 * @author Arvid Heise
 */
public abstract class AbstractIterable<I, O> implements Iterable<O> {
	private final Iterable<I> originalIterable;

	public AbstractIterable(final Iterable<I> originalIterable) {
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
	 * @param iterator
	 * @return
	 */
	protected abstract Iterator<O> wrap(Iterator<I> iterator);

}
