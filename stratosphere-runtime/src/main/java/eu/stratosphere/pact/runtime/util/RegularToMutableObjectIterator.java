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

package eu.stratosphere.pact.runtime.util;

import java.util.Iterator;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.util.MutableObjectIterator;


public class RegularToMutableObjectIterator<T> implements MutableObjectIterator<T> {

	private final Iterator<T> iterator;
	
	private final TypeSerializer<T> serializer;

	public RegularToMutableObjectIterator(Iterator<T> iterator, TypeSerializer<T> serializer) {
		this.iterator = iterator;
		this.serializer = serializer;
	}

	@Override
	public T next(T reuse) {
		// -----------------------------------------------------------------------------------------
		// IMPORTANT: WE NEED TO COPY INTO THE REUSE OBJECT TO SIMULATE THE MUTABLE OBJECT RUNTIME
		// -----------------------------------------------------------------------------------------
		if (this.iterator.hasNext()) {
			return this.serializer.copy(this.iterator.next(), reuse);
		} else {
			return null;
		}
	}
}
