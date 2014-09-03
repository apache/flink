/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators.util;

import java.io.IOException;

import org.apache.flink.util.MutableObjectIterator;


/**
 * A simple iterator provider that returns a supplied iterator and does nothing when closed.
 *
 */
public class SimpleCloseableInputProvider<E> implements CloseableInputProvider<E>
{
	/**
	 * The iterator returned by this class.
	 */
	private final MutableObjectIterator<E> iterator;
	
	
	/**
	 * Creates a new simple input provider that will return the given iterator.
	 * 
	 * @param iterator The iterator that will be returned.
	 */
	public SimpleCloseableInputProvider(MutableObjectIterator<E> iterator) {
		this.iterator = iterator;
	}
	
	

	@Override
	public void close() throws IOException {
		// do nothing
	}


	@Override
	public MutableObjectIterator<E> getIterator() {
		return this.iterator;
	}

}
