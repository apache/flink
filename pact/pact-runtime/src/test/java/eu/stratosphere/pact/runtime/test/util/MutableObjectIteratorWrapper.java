/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.runtime.test.util;

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.MutableObjectIterator;


/**
 *
 */
public class MutableObjectIteratorWrapper implements MutableObjectIterator<PactRecord>
{
	private final Iterator<PactRecord> source;
	
	public MutableObjectIteratorWrapper(Iterator<PactRecord> source)
	{
		this.source = source;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.util.MutableObjectIterator#next(java.lang.Object)
	 */
	@Override
	public boolean next(PactRecord target) throws IOException {
		if (this.source.hasNext()) {
			this.source.next().copyTo(target);
			return true;
		}
		else {
			return false;
		}
	}

}
