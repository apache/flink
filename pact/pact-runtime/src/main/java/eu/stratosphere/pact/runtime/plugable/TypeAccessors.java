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

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.type.Key;


/**
 *
 *
 * @author Stephan Ewen
 */
public interface TypeAccessors<T>
{
	public T createInstance();
	
	public T createCopy(T from);
	
	public void copyTo(T from, T to);
	
	// --------------------------------------------------------------------------------------------
	
	public long serialize(T record, DataOutputView target, Iterator<MemorySegment> furtherBuffers, List<MemorySegment> targetForUsedFurther) throws IOException;
	
	public void deserialize(T target, List<MemorySegment> sources, int firstSegment, int segmentOffset) throws IOException;  
	
	// --------------------------------------------------------------------------------------------
	
	public int hash(T object);
	
	public int compare(T first, T second, Comparator<Key> comparator);
	
	public int compare(T holder1, T holder2, List<MemorySegment> sources1, List<MemorySegment> sources2, int firstSegment1, int firstSegment2, int offset1, int offset2);
	
	// --------------------------------------------------------------------------------------------
	
	public boolean supportsNormalizedKey();
	
	public int getNormalizeKeyLen();
	
	public boolean isNormalizedKeyPrefixOnly(int keyBytes);
	
	public void putNormalizedKey(T record, byte[] target, int offset, int numBytes);
}
