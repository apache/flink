/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.util;

import java.io.IOException;

/**
 * A simple iterator interface. The key differences to the {@link java.util.Iterator} are that this
 * iterator accepts an object into which it can place the content if the object is mutable, and that
 * it consolidates the logic in a single <code>next()</code> function, rather than in two different
 * functions such as <code>hasNext()</code> and <code>next()</code>.
 * 
 * @param <E> The element type of the collection iterated over.
 */
public interface MutableObjectIterator<E> {
	
	/**
	 * Gets the next element from the collection. The contents of that next element is put into the given target object.
	 * 
	 * @param reuse The target object into which to place next element if E is mutable.
	 * @return The filled object or <code>null</code> if the iterator is exhausted
	 * 
	 * @throws IOException Thrown, if a problem occurred in the underlying I/O layer or in the 
	 *                     serialization / deserialization logic
	 */
	public E next(E reuse) throws IOException;
}
