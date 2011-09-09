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

package eu.stratosphere.pact.runtime.plugable;

import java.util.Comparator;
import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Key;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public interface TypeAccessors<T>
{
	public T createInstance();
	
	public T createCopy(T from);
	
	public void copyTo(T from, T to);
	
	// --------------------------------------------------------------------------------------------
	
	public long serialize(T record, DataOutputView target, List<MemorySegment> furtherBuffers, List<MemorySegment> targetForUsedFurther);
	
	// --------------------------------------------------------------------------------------------
	
	public int hash(T object);
	
	public int compare(T first, T second, Comparator<Key> comparator);
	
	// --------------------------------------------------------------------------------------------
	
	public boolean supportNormalizedKey();
	
	public int getNormalizeKeyLen();
	
	public void putNormalizedKey(byte[] target, int numBytes);
}
