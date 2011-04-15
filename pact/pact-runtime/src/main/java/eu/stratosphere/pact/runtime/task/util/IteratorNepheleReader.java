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

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.types.Record;

/**
 * Wraps a Java Iterator into a Nephele reader.
 * 
 * @see Reader
 * @see Iterator
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 */
public class IteratorNepheleReader<T extends Record> implements Reader<T> {

	private final Iterator<T> it;
	
	public IteratorNepheleReader(Iterator<T> it) {
		this.it = it;
	}
	
	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

	@Override
	public T next() throws IOException,
			InterruptedException {
		return it.next();
	}

	
}
