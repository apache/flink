/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.api.java.io;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import eu.stratosphere.api.common.io.GenericInputFormat;
import eu.stratosphere.api.common.io.UnsplittableInput;
import eu.stratosphere.core.io.GenericInputSplit;

/**
 * An input format that returns objects from a collection.
 */
public class CollectionInputFormat<T> extends GenericInputFormat<T> implements UnsplittableInput {

	private static final long serialVersionUID = 1L;

	private final Collection<T> dataSet; // input data as collection

	private transient Iterator<T> iterator;
	

	
	public CollectionInputFormat(Collection<T> dataSet) {
		if (dataSet == null)
			throw new NullPointerException();
		
		this.dataSet = dataSet;
	}

	
	@Override
	public boolean reachedEnd() throws IOException {
		return !this.iterator.hasNext();
	}

	@Override
	public void open(GenericInputSplit split) throws IOException {
		super.open(split);
		
		this.iterator = this.dataSet.iterator();
	}
	
	@Override
	public T nextRecord(T record) throws IOException {
		return this.iterator.next();
	}
	
	// --------------------------------------------------------------------------------------------
	
	@Override
	public String toString() {
		return this.dataSet.toString();
	}
	
	// --------------------------------------------------------------------------------------------
	
	public static <X> void checkCollection(Collection<X> elements, Class<X> viewedAs) {
		if (elements == null || viewedAs == null) {
			throw new NullPointerException();
		}
		
		for (X elem : elements) {
			if (elem == null) {
				throw new IllegalArgumentException("The collection must not contain null elements.");
			}
			
			if (!viewedAs.isAssignableFrom(elem.getClass())) {
				throw new IllegalArgumentException("The elements in the collection are not all subclasses of " + 
							viewedAs.getCanonicalName());
			}
		}
	}
}
