/***********************************************************************************************************************
*
* Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.util.NoSuchElementException;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;


public class MutableToRegularIteratorWrapper implements Iterator<PactRecord>
{
	private final MutableObjectIterator<PactRecord> source;
	
	private final PactRecord instance;
	
	private PactRecord staged;

	public MutableToRegularIteratorWrapper(MutableObjectIterator<PactRecord> source) {
		this.source = source;
		this.instance = new PactRecord();
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		if (this.staged == null) {
			try {
				if (this.source.next(this.instance)) {
					this.staged = this.instance;
					return true;
				} else {
					return false;
				}
			} catch (IOException ioex) {
				throw new RuntimeException("Error reading next record: " + ioex.getMessage(), ioex);
			}
		} else {
			return true;
		}
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public PactRecord next() {
		if (this.staged != null || hasNext()) {
			final PactRecord rec = this.staged;
			this.staged = null;
			return rec;
		} else {
			throw new NoSuchElementException();
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
