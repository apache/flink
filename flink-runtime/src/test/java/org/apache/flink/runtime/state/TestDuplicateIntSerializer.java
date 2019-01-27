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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.ParameterlessTypeSerializerConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Integer serializer to test whether the serializer is duplicated for async snapshot.
 * The duplicated serializer will throw exception when it is used to serialize an integer.
 */
public final class TestDuplicateIntSerializer extends TypeSerializer<Integer> {

	private static final long serialVersionUID = 1L;

	private static final Integer ZERO = Integer.valueOf(0);

	private final boolean throwException;

	public TestDuplicateIntSerializer() {
		this(false);
	}

	private TestDuplicateIntSerializer(boolean throwException) {
		this.throwException = throwException;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public TypeSerializer<Integer> duplicate() {
		return new TestDuplicateIntSerializer(true);
	}

	@Override
	public Integer createInstance() {
		return ZERO;
	}

	@Override
	public Integer copy(Integer from) {
		return from;
	}

	@Override
	public Integer copy(Integer from, Integer reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 4;
	}

	@Override
	public void serialize(Integer record, DataOutputView target) throws IOException {
		checkException();
		target.writeInt(record.intValue());
	}

	@Override
	public Integer deserialize(DataInputView source) throws IOException {
		return Integer.valueOf(source.readInt());
	}

	@Override
	public Integer deserialize(Integer reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeInt(source.readInt());
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof TestDuplicateIntSerializer;
	}

	@Override
	public TypeSerializerConfigSnapshot snapshotConfiguration() {
		// type serializer singletons should always be parameter-less
		return new ParameterlessTypeSerializerConfig(getSerializationFormatIdentifier());
	}

	@Override
	public CompatibilityResult<Integer> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof ParameterlessTypeSerializerConfig
			&& isCompatibleSerializationFormatIdentifier(
			((ParameterlessTypeSerializerConfig) configSnapshot).getSerializationFormatIdentifier())) {

			return CompatibilityResult.compatible();
		} else {
			return CompatibilityResult.requiresMigration();
		}
	}

	@Override
	public int hashCode() {
		return this.getClass().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof TestDuplicateIntSerializer) {
			TestDuplicateIntSerializer other = (TestDuplicateIntSerializer) obj;

			return other.canEqual(this) && other.throwException == throwException;
		} else {
			return false;
		}
	}

	private void checkException() {
		if (throwException) {
			throw new DuplicateSerializerException();
		}
	}

	private boolean isCompatibleSerializationFormatIdentifier(String identifier) {
		return identifier.equals(getSerializationFormatIdentifier());
	}

	private String getSerializationFormatIdentifier() {
		return getClass().getCanonicalName();
	}

	/**
	 * Signals that a serializer is duplicated.
	 */
	static class DuplicateSerializerException extends RuntimeException {

	}
}
