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


public class ShortSerializer extends TypeSerializer<Short> implements ImmutableTypeUtil {

	private static final long serialVersionUID = 1L;
	
	public static final ShortSerializer INSTANCE = new ShortSerializer();
	
	private static final Short ZERO = Short.valueOf((short)0);


	@Override
	public Short createInstance() {
		return ZERO;
	}

	@Override
	public Short copy(Short from, Short reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 2;
	}

	@Override
	public void serialize(Short record, DataOutputView target) throws IOException {
		target.writeShort(record.shortValue());
	}

	@Override
	public Short deserialize(Short reuse, DataInputView source) throws IOException {
		return Short.valueOf(source.readShort());
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeShort(source.readShort());
	}
}
