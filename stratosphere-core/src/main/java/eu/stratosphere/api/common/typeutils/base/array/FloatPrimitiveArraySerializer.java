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
package eu.stratosphere.api.common.typeutils.base.array;

import java.io.IOException;

import eu.stratosphere.api.common.typeutils.TypeSerializer;
import eu.stratosphere.core.memory.DataInputView;
import eu.stratosphere.core.memory.DataOutputView;

/**
 * A serializer for long arrays.
 */
public class FloatPrimitiveArraySerializer extends TypeSerializer<float[]>{

	private static final long serialVersionUID = 1L;
	
	private static final float[] EMPTY = new float[0];

	
	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public boolean isStateful() {
		return false;
	}
	
	@Override
	public float[] createInstance() {
		return EMPTY;
	}

	@Override
	public float[] copy(float[] from, float[] reuse) {
		float[] copy = new float[from.length];
		System.arraycopy(from, 0, copy, 0, from.length);
		return copy;
	}

	@Override
	public int getLength() {
		return -1;
	}


	@Override
	public void serialize(float[] record, DataOutputView target) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("The record must not be null.");
		}
		
		final int len = record.length;
		target.writeInt(len);
		for (int i = 0; i < len; i++) {
			target.writeFloat(record[i]);
		}
	}


	@Override
	public float[] deserialize(float[] reuse, DataInputView source) throws IOException {
		final int len = source.readInt();
		reuse = new float[len];
		
		for (int i = 0; i < len; i++) {
			reuse[i] = source.readFloat();
		}
		
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		final int len = source.readInt();
		target.writeInt(len);
		target.write(source, len * 4);
	}
}
