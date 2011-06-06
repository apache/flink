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

package eu.stratosphere.pact.runtime.task.util;

import java.util.Iterator;

import eu.stratosphere.nephele.io.IOReadableWritable;
import eu.stratosphere.nephele.services.iomanager.SerializationFactory;

/**
 * Utility class that turns a standard {@link java.util.Iterator} for key/value pairs into a
 * {@link LastRepeatableIterator}. 
 */
public class RepeatableIteratorWrapper<E extends IOReadableWritable> implements LastRepeatableIterator<E>
{
	private final SerializationFactory<E> serializationFactory;
	
	private final SerializationCopier<E> copier;
	
	private final Iterator<E> input;
	
	public RepeatableIteratorWrapper(Iterator<E> input, SerializationFactory<E> serializationFactory)
	{
		this.input = input;
		this.serializationFactory = serializationFactory;
		this.copier = new SerializationCopier<E>();
	}
	
	@Override
	public boolean hasNext() {
		return this.input.hasNext();
	}

	@Override
	public E next() {
		E localNext = this.input.next();
		// serialize pair
		this.copier.setCopy(localNext);	
		return localNext;
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	@Override
	public E repeatLast() {
		E localNext = this.serializationFactory.newInstance();
		this.copier.getCopy(localNext);	
		return localNext;
	}
}