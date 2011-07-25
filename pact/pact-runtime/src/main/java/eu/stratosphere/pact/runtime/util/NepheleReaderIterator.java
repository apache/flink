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

package eu.stratosphere.pact.runtime.util;

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.nephele.io.Reader;
import eu.stratosphere.nephele.types.Record;

/**
 * Wraps a Nephele reader into a Java Iterator.
 * 
 * @see Reader
 * @see Iterator
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 */
public class NepheleReaderIterator<T extends Record> implements Iterator<T> {

	private final Reader<T> reader;		// the reader who's input is encapsulated in the iterator
	
	public NepheleReaderIterator(Reader<T> reader) {
		this.reader = reader;
	}
	
	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return reader.hasNext();
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public T next() {
		try {
			return reader.next();
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		} catch (InterruptedException ie) {
			throw new RuntimeException(ie);
		}
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
