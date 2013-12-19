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

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.types.IntValue;


public class IntValueSerializer extends TypeSerializer<IntValue> {

	@Override
	public IntValue createInstance() {
		return new IntValue();
	}

	@Override
	public IntValue createCopy(IntValue from) {
		return new IntValue(from.getValue());
	}

	@Override
	public void copyTo(IntValue from, IntValue to) {
		to.setValue(from.getValue());
	}


	@Override
	public int getLength() {
		return 4;
	}

	@Override
	public void serialize(IntValue record, DataOutputView target) throws IOException {
		target.writeInt(record.getValue());
	}

	@Override
	public void deserialize(IntValue target, DataInputView source) throws IOException {
		target.setValue(source.readInt());
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 4);
	}
}
