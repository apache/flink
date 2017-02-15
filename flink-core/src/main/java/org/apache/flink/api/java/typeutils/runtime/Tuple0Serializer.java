/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.flink.api.java.typeutils.runtime;

import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

@Internal
public class Tuple0Serializer extends TupleSerializer<Tuple0> {
	
	private static final long serialVersionUID = 1278813169022975971L;

	public static final Tuple0Serializer INSTANCE = new Tuple0Serializer();

	// ------------------------------------------------------------------------
	
	private Tuple0Serializer() {
		super(Tuple0.class, new TypeSerializer<?>[0]);
	}

	// ------------------------------------------------------------------------

	@Override
	public Tuple0Serializer duplicate() {
		return this;
	}

	@Override
	public Tuple0 createInstance() {
		return Tuple0.INSTANCE;
	}

	@Override
	public Tuple0 createInstance(Object[] fields) {
		if (fields == null || fields.length == 0) {
			return Tuple0.INSTANCE;
		}

		throw new UnsupportedOperationException(
				"Tuple0 cannot take any data, as it has zero fields.");
	}

	@Override
	public Tuple0 copy(Tuple0 from) {
		return from;
	}

	@Override
	public Tuple0 copy(Tuple0 from, Tuple0 reuse) {
		return reuse;
	}

	@Override
	public int getLength() {
		return 1;
	}

	@Override
	public void serialize(Tuple0 record, DataOutputView target) throws IOException {
		target.writeByte(42);
	}

	@Override
	public Tuple0 deserialize(DataInputView source) throws IOException {
		source.readByte();
		return Tuple0.INSTANCE;
	}

	@Override
	public Tuple0 deserialize(Tuple0 reuse, DataInputView source) throws IOException {
		source.readByte();
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeByte(source.readByte());
	}

	// ------------------------------------------------------------------------


	@Override
	public int hashCode() {
		return Tuple0Serializer.class.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Tuple0Serializer) {
			Tuple0Serializer other = (Tuple0Serializer) obj;

			return other.canEqual(this);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof Tuple0Serializer;
	}

	@Override
	public String toString() {
		return "Tuple0Serializer";
	}
}
