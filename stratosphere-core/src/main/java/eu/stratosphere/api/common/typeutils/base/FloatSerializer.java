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

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

import java.io.IOException;


public class FloatSerializer extends TypeSerializer<Float> {

	private static final long serialVersionUID = 1L;
	
	public static final FloatSerializer INSTANCE = new FloatSerializer();
	
	private static final Float ZERO = Float.valueOf(0);

	
	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public boolean isStateful() {
		return false;
	}

	@Override
	public Float createInstance() {
		return ZERO;
	}

	@Override
	public Float copy(Float from, Float reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 4;
	}

	@Override
	public void serialize(Float record, DataOutputView target) throws IOException {
		target.writeFloat(record.floatValue());
	}

	@Override
	public Float deserialize(Float reuse, DataInputView source) throws IOException {
		return Float.valueOf(source.readFloat());
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeFloat(source.readFloat());
	}
}
