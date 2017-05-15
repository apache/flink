/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.runtime.operators.testutils.types;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;


public class IntPairSerializer extends TypeSerializer<IntPair> {
	
	private static final long serialVersionUID = 1L;

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public IntPairSerializer duplicate() {
		return this;
	}

	@Override
	public IntPair createInstance() {
		return new IntPair();
	}
	
	@Override
	public IntPair copy(IntPair from) {
		return new IntPair(from.getKey(), from.getValue());
	}

	@Override
	public IntPair copy(IntPair from, IntPair reuse) {
		reuse.setKey(from.getKey());
		reuse.setValue(from.getValue());
		return reuse;
	}
	

	@Override
	public int getLength() {
		return 8;
	}

	@Override
	public void serialize(IntPair record, DataOutputView target) throws IOException {
		target.writeInt(record.getKey());
		target.writeInt(record.getValue());
	}

	@Override
	public IntPair deserialize(DataInputView source) throws IOException {
		return new IntPair(source.readInt(), source.readInt());
	}
	
	@Override
	public IntPair deserialize(IntPair reuse, DataInputView source) throws IOException {
		reuse.setKey(source.readInt());
		reuse.setValue(source.readInt());
		return reuse;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.write(source, 8);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof IntPairSerializer) {
			IntPairSerializer other = (IntPairSerializer) obj;

			return other.canEqual(this);
		} else {
			return false;
		}
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof IntPairSerializer;
	}

	@Override
	public int hashCode() {
		return IntPairSerializer.class.hashCode();
	}

	public static final class IntPairSerializerFactory implements TypeSerializerFactory<IntPair> {

		@Override
		public void writeParametersToConfig(Configuration config) {}

		@Override
		public void readParametersFromConfig(Configuration config, ClassLoader cl) {}

		@Override
		public IntPairSerializer getSerializer() {
			return new IntPairSerializer();
		}

		@Override
		public Class<IntPair> getDataType() {
			return IntPair.class;
		}

		@Override
		public int hashCode() {
			return 42;
		}

		public boolean equals(Object obj) {
			return obj.getClass() == IntPairSerializerFactory.class;
		};
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		throw new UnsupportedOperationException();
	}

	@Override
	public CompatibilityResult<IntPair> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		throw new UnsupportedOperationException();
	}
}
