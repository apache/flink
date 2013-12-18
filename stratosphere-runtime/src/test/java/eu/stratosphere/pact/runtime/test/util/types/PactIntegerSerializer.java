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

package eu.stratosphere.pact.runtime.test.util.types;

import java.io.IOException;

import eu.stratosphere.api.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.types.PactInteger;

/**
 * @author Stephan Ewen
 */
public class PactIntegerSerializer extends TypeSerializer<PactInteger> {


	@Override
	public PactInteger createInstance() {
		return new PactInteger();
	}


	@Override
	public PactInteger createCopy(PactInteger from) {
		return new PactInteger(from.getValue());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeSerializer#copyTo(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void copyTo(PactInteger from, PactInteger to) {
		to.setValue(from.getValue());
	}


	@Override
	public int getLength() {
		return 4;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeSerializer#serialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2)
	 */
	@Override
	public void serialize(PactInteger record, DataOutputView target) throws IOException {
		target.writeInt(record.getValue());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeSerializer#deserialize(java.lang.Object, eu.stratosphere.nephele.services.memorymanager.DataInputViewV2)
	 */
	@Override
	public void deserialize(PactInteger target, DataInputView source) throws IOException {
		target.setValue(source.readInt());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.runtime.plugable.TypeSerializer#copy(eu.stratosphere.nephele.services.memorymanager.DataInputViewV2, eu.stratosphere.nephele.services.memorymanager.DataOutputViewV2)
	 */
	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 4);
	}
}
