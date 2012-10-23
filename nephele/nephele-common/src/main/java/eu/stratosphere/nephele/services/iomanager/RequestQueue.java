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

package eu.stratosphere.nephele.services.iomanager;


import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;


/**
 * A {@link LinkedBlockingQueue} that is extended with closing methods.
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class RequestQueue<E> extends LinkedBlockingQueue<E> implements Closeable
{
	/**
	 * UID for serialization interoperability. 
	 */
	private static final long serialVersionUID = 3804115535778471680L;
	
	/**
	 * Flag marking this queue as closed.
	 */
	private volatile boolean closed = false;
	
	/**
	 * Closes this request queue.
	 * 
	 * @see java.io.Closeable#close()
	 */
	@Override
	public void close() throws IOException {
		this.closed = true;
	}
	
	/**
	 * Checks whether this request queue is closed.
	 * 
	 * @return True, if the queue is closed, false otherwise.
	 */
	public boolean isClosed()
	{
		return this.closed;
	}
	
}
