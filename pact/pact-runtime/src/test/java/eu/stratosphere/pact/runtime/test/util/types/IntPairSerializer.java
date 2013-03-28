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

package eu.stratosphere.pact.runtime.test.util.types;

import java.io.IOException;

import eu.stratosphere.nephele.services.memorymanager.DataInputView;
import eu.stratosphere.nephele.services.memorymanager.DataOutputView;
import eu.stratosphere.pact.generic.types.TypeSerializer;


public class IntPairSerializer extends TypeSerializer<IntPair>
{	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#createInstance()
	 */
	@Override
	public IntPair createInstance()
	{
		return new IntPair();
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#createCopy(java.lang.Object)
	 */
	@Override
	public IntPair createCopy(IntPair from)
	{
		return new IntPair(from.getKey(), from.getValue());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copyTo(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void copyTo(IntPair from, IntPair to)
	{
		to.setKey(from.getKey());
		to.setValue(from.getValue());
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#getLength()
	 */
	@Override
	public int getLength()
	{
		return 8;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#serialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
	 */
	@Override
	public void serialize(IntPair record, DataOutputView target) throws IOException
	{
		target.writeInt(record.getKey());
		target.writeInt(record.getValue());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#deserialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataInputView)
	 */
	@Override
	public void deserialize(IntPair target, DataInputView source) throws IOException {
		target.setKey(source.readInt());
		target.setValue(source.readInt());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2#copy(eu.stratosphere.nephele.services.memorymanager.DataInputView, eu.stratosphere.nephele.services.memorymanager.DataOutputView)
	 */
	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException
	{
		for (int i = 0; i < 8; i++) {
			target.writeByte(source.readUnsignedByte());
		}
	}
}
