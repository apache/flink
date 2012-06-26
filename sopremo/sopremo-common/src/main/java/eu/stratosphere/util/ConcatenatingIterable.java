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
import java.util.Iterator;

/**
 * Concatenates {@link Iterable}s on-the-fly.<br>
 * Changes to the parameters are directly reflected in the result.
 * 
 * @author Arvid Heise
 */
public class ConcatenatingIterable<T> extends AbstractIterable<T> {

	private Iterable<? extends Iterable<? extends T>> inputs;

	public ConcatenatingIterable(final Iterable<T>... iterables) {
		this.inputs = Arrays.asList(iterables);
	}

	public ConcatenatingIterable(final Iterable<? extends Iterable<? extends T>> iterables) {
		this.inputs = iterables;
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<T> iterator() {
		return new ConcatenatingIterator<T>(new ConversionIterator<Iterable<? extends T>, Iterator<? extends T>>(
			this.inputs.iterator()) {
			/*
			 * (non-Javadoc)
			 * @see eu.stratosphere.util.ConversionIterator#convert(java.lang.Object)
			 */
			@Override
			protected Iterator<? extends T> convert(Iterable<? extends T> inputObject) {
				return inputObject.iterator();
			}
		});
	}
}
