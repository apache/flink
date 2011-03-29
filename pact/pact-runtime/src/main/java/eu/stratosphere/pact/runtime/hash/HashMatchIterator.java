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

package eu.stratosphere.pact.runtime.hash;

import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.pact.runtime.task.util.MatchTaskIterator;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class HashMatchIterator<K extends Key, V1 extends Value, V2 extends Value> implements MatchTaskIterator<K, V1, V2>
{

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#open()
	 */
	@Override
	public void open() throws IOException, MemoryAllocationException, InterruptedException {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#close()
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#next()
	 */
	@Override
	public boolean next() throws IOException {
		// TODO Auto-generated method stub
		return false;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#getKey()
	 */
	@Override
	public K getKey() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#getValues1()
	 */
	@Override
	public Iterator<V1> getValues1() {
		// TODO Auto-generated method stub
		return null;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.task.util.MatchTaskIterator#getValues2()
	 */
	@Override
	public Iterator<V2> getValues2() {
		// TODO Auto-generated method stub
		return null;
	}

}
