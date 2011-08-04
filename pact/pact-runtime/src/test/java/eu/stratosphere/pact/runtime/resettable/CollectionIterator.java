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


package eu.stratosphere.pact.runtime.resettable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.runtime.util.MutableObjectIterator;


public class CollectionIterator implements MutableObjectIterator<PactRecord>
{
	private List<PactRecord> objects;

	private int position = 0;

	public CollectionIterator(Collection<PactRecord> objects) {
		this.objects = new ArrayList<PactRecord>(objects);
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public boolean next(PactRecord target)
	{
		if (position < objects.size()) {
			this.objects.get(position++).copyTo(target);
			return true;
		}
		return false;
	}
}