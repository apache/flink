/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.pact.common.generic.types.TypeSerializer;
import eu.stratosphere.pact.common.type.PactRecord;


/**
 * Implementation of the (de)serialization and copying logic for the {@link PactRecord}.
 *
 * @author Stephan Ewen
 */
public final class PactRecordSerializer implements TypeSerializer<PactRecord>
{
	private static final PactRecordSerializer INSTANCE = new PactRecordSerializer(); // singleton instance
	
	private static final int MAX_BIT = 0x80;	// byte where only the most significant bit is set
	
	// --------------------------------------------------------------------------------------------

	public static final PactRecordSerializer get() {
		return INSTANCE;
	}
	
	/**
	 * Creates a new instance of the PactRecordSerializers. Private to prevent instantiation.
	 */
	private PactRecordSerializer()
	{}

	// --------------------------------------------------------------------------------------------
	
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
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#getLength()
	 */
	@Override
	public int getLength() {
		return -1;
	}

	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#serialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2)
	 */
	@Override
	public long serialize(PactRecord record, DataOutputView target) throws IOException
	{
		return record.serialize(target);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#deserialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataInputViewV2)
	 */
	@Override
	public void deserialize(PactRecord target, DataInputView source) throws IOException
	{
		target.deserialize(source);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copy(eu.stratosphere.nephele.services.memorymanager.DataInputViewV2, eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2)
	 */
	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException
	{
		int val = source.readUnsignedByte();
		target.writeByte(val);
		
		if (val >= MAX_BIT) {
			int shift = 7;
			int curr;
			val = val & 0x7f;
			while ((curr = source.readUnsignedByte()) >= MAX_BIT) {
				target.writeByte(curr);
				val |= (curr & 0x7f) << shift;
				shift += 7;
			}
			target.writeByte(curr);
			val |= curr << shift;
		}
		
		target.write(source, val);
	}
}
