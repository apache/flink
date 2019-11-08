/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.typeutils;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InvalidClassException;
import java.util.Arrays;

/**
 * Dummy TypeSerializer to avoid that data is lost when checkpointing again a serializer for which we encountered
 * a {@link ClassNotFoundException} or {@link InvalidClassException}.
 */
public class UnloadableDummyTypeSerializer<T> extends TypeSerializer<T> {

	private static final long serialVersionUID = 2526330533671642711L;

	private final byte[] actualBytes;

	@Nullable
	private final Throwable originalError;

	public UnloadableDummyTypeSerializer(byte[] actualBytes) {
		this(actualBytes, null);
	}

	public UnloadableDummyTypeSerializer(byte[] actualBytes, @Nullable Throwable originalError) {
		this.actualBytes = Preconditions.checkNotNull(actualBytes);
		this.originalError = originalError;
	}

	public byte[] getActualBytes() {
		return actualBytes;
	}

	@Nullable
	public Throwable getOriginalError() {
		return originalError;
	}

	@Override
	public boolean isImmutableType() {
		throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
	}

	@Override
	public TypeSerializer<T> duplicate() {
		throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
	}

	@Override
	public T createInstance() {
		throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
	}

	@Override
	public T copy(T from) {
		throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
	}

	@Override
	public T copy(T from, T reuse) {
		throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
	}

	@Override
	public int getLength() {
		throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
	}

	@Override
	public TypeSerializerSnapshot<T> snapshotConfiguration() {
		throw new UnsupportedOperationException("This object is a dummy TypeSerializer.");
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}

		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		UnloadableDummyTypeSerializer<?> that = (UnloadableDummyTypeSerializer<?>) o;

		return Arrays.equals(getActualBytes(), that.getActualBytes());
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(getActualBytes());
	}
}
