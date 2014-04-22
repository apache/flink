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
package eu.stratosphere.api.java.operators.translation;

import java.util.Iterator;

import eu.stratosphere.api.java.tuple.Tuple2;


/**
 *
 */
public class PeekingTupleUnwrappingIterator<T, K> implements Iterator<T>, java.io.Serializable {

	private static final long serialVersionUID = 1L;

	
	private Iterator<Tuple2<K, T>> iterator;
	
	private K key;

	private Tuple2<K, T> first;
	
	public void set(Iterator<Tuple2<K, T>> iterator) {
		this.iterator = iterator;
		
		// This construct is needed to provide a key for the TupleWrappingCollector in case of a specified 
		// combine method of group reduce with key selector function
		if(this.hasNext()) {
			this.first = iterator.next();
			this.key = this.first.f0;
		}
	}

	@Override
	public boolean hasNext() {
		if(this.first != null) {
			return true;
		}
		return iterator.hasNext();
	}

	@Override
	public T next() {
		if(this.first != null) {
			T val = this.first.f1;
			this.first = null;
			return val;
		}
		return iterator.next().f1;
	}
	
	public K getKey() {
		return this.key;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}
}
