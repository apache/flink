/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
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

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.types.StringValue;

public class StringPairSerializer extends TypeSerializer<StringPair> {

	@Override
	public StringPair createInstance() {
		return new StringPair();
	}
	
	@Override
	public StringPair copy(StringPair from, StringPair reuse) {
		reuse.setKey(from.getKey());
		reuse.setValue(from.getValue());
		return reuse;
	}

	public StringPair createCopy(StringPair from) {
		return new StringPair(from.getKey(), from.getValue());
	}

	public void copyTo(StringPair from, StringPair to) {
		to.setKey(from.getKey());
		to.setValue(from.getValue());
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(StringPair record, DataOutputView target)
			throws IOException {
		StringValue.writeString(record.getKey(), target);
		StringValue.writeString(record.getValue(), target);
	}

	@Override
	public StringPair deserialize(StringPair record, DataInputView source)
			throws IOException {
		record.setKey(StringValue.readString(source));
		record.setValue(StringValue.readString(source));
		return record;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target)
			throws IOException {
		StringValue.writeString(StringValue.readString(source), target);
		StringValue.writeString(StringValue.readString(source), target);
	}

}
