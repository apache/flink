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
 * An {@link Iterable} which takes the elements of another iterable and converts the elements on-the-fly.<br>
 * Changes to the parameters are directly reflected in the result.
 * 
 * @author Arvid Heise
 * @param <From>
 *        the type of the original iterator
 * @param <To>
 *        the return type after the conversion
 */
public abstract class ConversionIterable<From, To> extends WrappingIterable<From, To> {

	/**
	 * Initializes ConversionIterable.
	 * 
	 * @param originalIterable
	 */
	public ConversionIterable(Iterable<? extends From> originalIterable) {
		super(originalIterable);
	}

	/**
	 * Convert the given object to the desired return type.
	 * 
	 * @param inputObject
	 *        the object to convert
	 * @return the result of the conversion of one object
	 */
	protected abstract To convert(From inputObject);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.util.WrappingIterable#wrap(java.util.Iterator)
	 */
	@Override
	protected Iterator<To> wrap(Iterator<? extends From> iterator) {
		return new ConversionIterator<From, To>(iterator) {
			@Override
			protected To convert(From inputObject) {
				return ConversionIterable.this.convert(inputObject);
			};
		};
	}
}
