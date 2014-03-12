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

import eu.stratosphere.api.common.typeutils.ImmutableTypeUtil;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;


public class CharSerializer extends TypeSerializer<Character> implements ImmutableTypeUtil {

	private static final long serialVersionUID = 1L;
	
	public static final CharSerializer INSTANCE = new CharSerializer();
	
	private static final Character ZERO = Character.valueOf((char)0);


	@Override
	public Character createInstance() {
		return ZERO;
	}

	@Override
	public Character copy(Character from, Character reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 2;
	}

	@Override
	public void serialize(Character record, DataOutputView target) throws IOException {
		target.writeChar(record.charValue());
	}

	@Override
	public Character deserialize(Character reuse, DataInputView source) throws IOException {
		return Character.valueOf(source.readChar());
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeChar(source.readChar());
	}
}
