/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.nephele.util;

import java.util.concurrent.atomic.AtomicInteger;


/**
 */
public class AtomicEnumerator<E>
{
	private final E[] values;
	
	private final AtomicInteger next;
	
	
	public AtomicEnumerator(E[] values)
	{
		this.values = values;
		this.next = new AtomicInteger(0);
	}
	
	
	public E getNext()
	{
		int n, nv;
		do {
			n = this.next.get();
			nv = n+1; 
		} while (!this.next.compareAndSet(n,  nv == this.values.length ? 0 : nv));
		
		return this.values[n];
	}
	
	
	public static final <T> AtomicEnumerator<T> get(T[] values)
	{
		return new AtomicEnumerator<T>(values);
	}
}
