/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/
package eu.stratosphere.api.common.typeutils.base;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;
import eu.stratosphere.types.StringValue;


public class StringSerializer extends TypeSerializer<String> {

	private static final long serialVersionUID = 1L;
	
	public static final StringSerializer INSTANCE = new StringSerializer();
	
	private static final String EMPTY = "";

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	public String createInstance() {
		return EMPTY;
	}

	@Override
	public String copy(String from, String reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(String record, DataOutputView target) throws IOException {
		StringValue.writeUnicodeString(record, target);
	}

	@Override
	public String deserialize(String record, DataInputView source) throws IOException {
		return StringValue.readUnicodeString(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		StringValue.copyUnicodeString(source, target);
	}
}
