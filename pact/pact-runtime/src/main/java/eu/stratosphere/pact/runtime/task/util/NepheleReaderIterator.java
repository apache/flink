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

import eu.stratosphere.nephele.io.RecordReader;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;


/**
 * A {@link MutableObjectIterator} that wraps a Nephele Reader producing {@link PactRecord}s.
 *
 * @author Stephan Ewen
 */
public final class NepheleReaderIterator implements MutableObjectIterator<PactRecord>
{
	private final RecordReader<PactRecord> reader;		// the source

	/**
	 * Creates a new iterator, wrapping the given reader.
	 * 
	 * @param reader The reader to wrap.
	 */
	public NepheleReaderIterator(RecordReader<PactRecord> reader)
	{
		this.reader = reader;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.util.MutableObjectIterator#next(java.lang.Object)
	 */
	@Override
	public boolean next(PactRecord target) throws IOException
	{
		if (this.reader.hasNext()) {
			try {
				PactRecord rec = this.reader.next();
				rec.copyTo(target);
				return true;
			}
			catch (InterruptedException e) {
				throw new IOException("The reading thread has been interrupted.", e);
			}
		}
		else {
			return false;
		}
	}
	
	
}
