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

import java.io.Closeable;
import java.util.Iterator;


/**
 * Utility interface for a provider of an input that can be closed.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface CloseableInputProvider<E> extends Closeable
{
	
	/**
	 * Gets the iterator over this input.
	 * 
	 * @return The iterator provided by this iterator provider.
	 * @throws InterruptedException 
	 */
	public Iterator<E> getIterator() throws InterruptedException;
}
