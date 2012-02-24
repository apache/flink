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

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * @author Arvid Heise
 */
public class ConcatenatingIterable<T> implements Iterable<T> {

	private Iterable<Iterable<T>> inputs;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public ConcatenatingIterable(final Iterable<?>... iterables) {
		this.inputs = new LinkedList<Iterable<T>>((Collection) Arrays.asList(iterables));
	}

	public ConcatenatingIterable(final Iterable<Iterable<T>> iterables) {
		this.inputs = iterables;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<T> iterator() {
		return new ConcatenatingIterator<T>(new ConversionIterator<Iterable<T>, Iterator<T>>(
			this.inputs.iterator()) {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.util.ConversionIterator#convert(java.lang.Object)
			 */
			@Override
			protected Iterator<T> convert(Iterable<T> inputObject) {
				return inputObject.iterator();
			}
		});
	}
}
