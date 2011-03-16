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
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Wraps a Nephele KeyValuePair reader into a Java Iterator.
 * 
 * @see KeyValuePair
 * @see Reader
 * @see Iterator
 * 
 * @author Fabian Hueske (fabian.hueske@tu-berlin.de)
 *
 * @param <K>
 * @param <V>
 */
public class NepheleReaderIterator<K extends Key, V extends Value> implements Iterator<KeyValuePair<K, V>> {

	private Reader<KeyValuePair<K,V>> reader;
	
	public NepheleReaderIterator(Reader<KeyValuePair<K,V>> reader) {
		this.reader = reader;
	}
	
	@Override
	public boolean hasNext() {
		return reader.hasNext();
	}

	@Override
	public KeyValuePair<K, V> next() {
		try {
			return reader.next();
		} catch (IOException ioe) {
			throw new RuntimeException(ioe);
		} catch (InterruptedException ie) {
			throw new RuntimeException(ie);
		}
	}

	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

}
