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
import eu.stratosphere.pact.common.type.NormalizableKey;
import eu.stratosphere.pact.common.type.NullKeyFieldException;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.util.InstantiationUtil;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public final class PactRecordAccessors implements TypeAccessors<PactRecord>
{
	private final int[] keyFields;
	
	private final Class<? extends Key>[] keyTypes;
	
	private final Key[] keyHolders1, keyHolders2;
	
	private final PactRecord holder1, holder2;
	
	private final int[] normalizedKeyLengths;
	
	private final int numLeadingNormalizableKeys;
	
	private final int normalizableKeyPrefixLen;
	
	/**
	 * @param keyFields
	 */
	public PactRecordAccessors(int[] keyFields, Class<? extends Key>[] keyTypes)
	{
		this.keyFields = keyFields;
		this.keyTypes = keyTypes;
		
		this.holder1 = new PactRecord();
		this.holder2 = new PactRecord();
		
		// instantiate fields to extract keys into
		this.keyHolders1 = new Key[keyTypes.length];
		this.keyHolders2 = new Key[keyTypes.length];
		for (int i = 0; i < keyTypes.length; i++) {
			if (keyTypes[i] == null) {
				throw new NullPointerException("Key type " + i + " is null.");
			}
			this.keyHolders1[i] = InstantiationUtil.instantiate(keyTypes[i], Key.class);
			this.keyHolders2[i] = InstantiationUtil.instantiate(keyTypes[i], Key.class);
		}
		
		// set up auxiliary fields for normalized key support
		this.normalizedKeyLengths = new int[keyFields.length];
		int nKeys = 0;
		int nKeyLen = 0;
		for (int i = 0; i < this.keyHolders1.length; i++) {
			Key k = this.keyHolders1[i];
			if (k instanceof NormalizableKey) {
				nKeys++;
				final int len = ((NormalizableKey) k).getMaxNormalizedKeyLen();
				if (len < 0) {
					throw new RuntimeException("Data type " + k.getClass().getName() + 
						" specifies an invalid length for the normalized key: " + len);
				}
				this.normalizedKeyLengths[i] = len;
				nKeyLen += this.normalizedKeyLengths[i];
				if (nKeyLen < 0) {
					nKeyLen = Integer.MAX_VALUE;
				}
			}
			else break;
		}
		this.numLeadingNormalizableKeys = nKeys;
		this.normalizableKeyPrefixLen = nKeyLen;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#createInstance()
	 */
	@Override
	public PactRecord createInstance() {
		return new PactRecord(); 
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#createCopy(java.lang.Object)
	 */
	@Override
	public PactRecord createCopy(PactRecord from) {
		return from.createCopy();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#copyTo(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void copyTo(PactRecord from, PactRecord to) {
		from.copyTo(to);
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#serialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataOutputView, java.util.List, java.util.List)
	 */
	@Override
	public long serialize(PactRecord record, DataOutputView target, Iterator<MemorySegment> furtherBuffers,
			List<MemorySegment> targetForUsedFurther)
	throws IOException
	{
		return record.serialize(record, target, furtherBuffers, targetForUsedFurther);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#deserialize(java.lang.Object, java.util.List, int, int)
	 */
	@Override
	public void deserialize(PactRecord target, List<MemorySegment> sources, int firstSegment, int segmentOffset)
	{
		target.deserialize(sources, firstSegment, segmentOffset);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#copy(java.util.List, int, int, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
	 */
	@Override
	public void copy(List<MemorySegment> sources, int firstSegment, int segmentOffset, DataOutputView target)
	throws IOException
	{
		MemorySegment seg = sources.get(firstSegment);
		int len = readLengthIncludingLengthBytes(seg, sources, firstSegment, segmentOffset);
		
		int remaining = seg.size() - segmentOffset;
		if (remaining >= len) {
			target.write(seg.getBackingArray(), seg.translateOffset(segmentOffset), len);
		}
		else while (true) {
			int toPut = Math.min(remaining, len);
			target.write(seg.getBackingArray(), seg.translateOffset(segmentOffset), toPut);
			len -= toPut;
			
			if (len > 0) {
				segmentOffset = 0;
				seg = sources.get(++firstSegment);
				remaining = seg.size();	
			}
			else {
				break;
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#hash(java.lang.Object)
	 */
	@Override
	public int hash(PactRecord object)
	{
		try {
			int code = 0;
			for (int i = 0; i < this.keyFields.length; i++) {
				code ^= object.getField(this.keyFields[i], this.keyTypes[i]).hashCode();
			}
			return code;
		}
		catch (NullPointerException npex) {
			throw new NullKeyFieldException();
		}
	}


	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#compare(java.lang.Object, java.lang.Object, java.util.Comparator)
	 */
	@Override
	public int compare(PactRecord first, PactRecord second, Comparator<Key> comparator)
	{
		if (first.getFieldsInto(this.keyFields, this.keyHolders1) &
		    second.getFieldsInto(this.keyFields, this.keyHolders2))
		{
			for (int i = 0; i < this.keyHolders1.length; i++) {
				int c = comparator.compare(this.keyHolders1[i], this.keyHolders2[i]);
				if (c != 0)
					return c;
			}
			return 0;
		}
		else throw new NullKeyFieldException();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#compare(java.util.List, java.util.List, int, int, int, int)
	 */
	@Override
	public int compare(List<MemorySegment> sources1, List<MemorySegment> sources2, int firstSegment1,
			int firstSegment2, int offset1, int offset2)
	{
		if (holder1.readBinary(this.keyFields, this.keyHolders1, sources1, firstSegment1, offset1) &
		    holder2.readBinary(this.keyFields, this.keyHolders2, sources2, firstSegment2, offset2))
		{
			for (int i = 0; i < this.keyHolders1.length; i++) {
				final int val = this.keyHolders1[i].compareTo(this.keyHolders2[i]);
				if (val != 0)
					return val;
			}
			return 0;
		}
		else throw new NullKeyFieldException();
	}
	
	// --------------------------------------------------------------------------------------------

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#supportsNormalizedKey()
	 */
	@Override
	public boolean supportsNormalizedKey()
	{
		return this.numLeadingNormalizableKeys > 0;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#getNormalizeKeyLen()
	 */
	@Override
	public int getNormalizeKeyLen()
	{
		return this.normalizableKeyPrefixLen;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#isNormalizedKeyPrefixOnly()
	 */
	@Override
	public boolean isNormalizedKeyPrefixOnly(int keyBytes)
	{
		return this.numLeadingNormalizableKeys < this.keyFields.length ||
		        this.normalizableKeyPrefixLen == Integer.MAX_VALUE ||
				this.normalizableKeyPrefixLen > keyBytes;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessors#putNormalizedKey(java.lang.Object, byte[], int, int)
	 */
	@Override
	public void putNormalizedKey(PactRecord record, byte[] target, int offset, int numBytes)
	{
		try {
			for (int i = 0; i < this.numLeadingNormalizableKeys & numBytes > 0; i++)
			{
				int len = this.normalizedKeyLengths[i]; 
				len = numBytes >= len ? len : numBytes;
				((NormalizableKey) record.getField(this.keyFields[i], this.keyTypes[i])).copyNormalizedKey(target, offset, len);
				numBytes -= len;
				offset += len;
			}
		}
		catch (NullPointerException npex) {
			throw new NullKeyFieldException();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	private static final int readLengthIncludingLengthBytes(MemorySegment seg, List<MemorySegment> sources, int segmentNum, int segmentOffset)
	{
		int lenBytes = 1;
		
		if (seg.size() - segmentOffset > 5) {
			int val = seg.get(segmentOffset++) & 0xff;
			if (val >= MAX_BIT) {
				int shift = 7;
				int curr;
				val = val & 0x7f;
				while ((curr = seg.get(segmentOffset++) & 0xff) >= MAX_BIT) {
					val |= (curr & 0x7f) << shift;
					shift += 7;
					lenBytes++;
				}
				val |= curr << shift;
			}
			return val + lenBytes;
		}
		else {
			int end = seg.size();
			int val = seg.get(segmentOffset++) & 0xff;
			if (segmentOffset == end) {
				segmentOffset = 0;
				seg = sources.get(++segmentNum);
			}
			
			if (val >= MAX_BIT) {
				int shift = 7;
				int curr;
				val = val & 0x7f;
				while ((curr = seg.get(segmentOffset++) & 0xff) >= MAX_BIT) {
					val |= (curr & 0x7f) << shift;
					shift += 7;
					lenBytes++;
					if (segmentOffset == end) {
						segmentOffset = 0;
						seg = sources.get(++segmentNum);
					}
				}
				val |= curr << shift;
			}
			return val + lenBytes;
		}
	}
		
	private static final int MAX_BIT = 0x1 << 7;
}
