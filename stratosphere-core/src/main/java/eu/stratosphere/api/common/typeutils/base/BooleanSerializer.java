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

import eu.stratosphere.api.common.typeutils.ImmutableTypeUtil;
import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

import java.io.IOException;


public class BooleanSerializer extends TypeSerializer<Boolean> implements ImmutableTypeUtil {

	private static final long serialVersionUID = 1L;
	
	public static final BooleanSerializer INSTANCE = new BooleanSerializer();
	
	private static final Boolean FALSE = Boolean.FALSE;


	@Override
	public Boolean createInstance() {
		return FALSE;
	}

	@Override
	public Boolean copy(Boolean from, Boolean reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 1;
	}

	@Override
	public void serialize(Boolean record, DataOutputView target) throws IOException {
		target.writeBoolean(record.booleanValue());
	}

	@Override
	public Boolean deserialize(Boolean reuse, DataInputView source) throws IOException {
		return Boolean.valueOf(source.readBoolean());
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeBoolean(source.readBoolean());
	}
}
