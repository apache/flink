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

import eu.stratosphere.nephele.services.memorymanager.MemoryAllocationException;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.Value;

public interface CoGroupTaskIterator<K extends Key, V1 extends Value, V2 extends Value> {
	
	/**
	 * General-purpose open method.
	 * 
	 * @throws IOException
	 * @throws MemoryAllocationException
	 * @throws InterruptedException 
	 */
	void open() throws IOException, MemoryAllocationException, InterruptedException;

	/**
	 * General-purpose close method.
	 */
	void close();

	/**
	 * Moves the internal pointer to the next key (if present).
	 * The key must NOT be shared by both inputs.
	 * In that case an empty iterator is returned by getValues1() or getValues2().
	 * Returns true if the operation was successful or false if no more keys are present.
	 * 
	 * @return true on success, false if no more keys are present
	 * @throws IOException
	 * @throws InterruptedException
	 */
	boolean next() throws IOException;

	/**
	 * Returns the current key.
	 * 
	 * @return Key the current key
	 */
	K getKey();

	/**
	 * Returns an iterable over the left input values for the current key.
	 * 
	 * @return an iterable over the left input values for the current key.
	 */
	Iterator<V1> getValues1();

	/**
	 * Returns an iterable over the left input values for the current key.
	 * 
	 * @return an iterable over the left input values for the current key.
	 */
	Iterator<V2> getValues2();
}
