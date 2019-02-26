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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;

public class SingleThreadAccessCheckingTypeSerializer<T> extends TypeSerializer<T> {
	private static final long serialVersionUID = 131020282727167064L;

	private final SingleThreadAccessChecker singleThreadAccessChecker;
	private final TypeSerializer<T> originalSerializer;

	public SingleThreadAccessCheckingTypeSerializer(TypeSerializer<T> originalSerializer) {
		this.singleThreadAccessChecker = new SingleThreadAccessChecker();
		this.originalSerializer = originalSerializer;
	}

	@Override
	public boolean isImmutableType() {
		singleThreadAccessChecker.checkSingleThreadAccess();
		return originalSerializer.isImmutableType();
	}

	@Override
	public TypeSerializer<T> duplicate() {
		singleThreadAccessChecker.checkSingleThreadAccess();
		return new SingleThreadAccessCheckingTypeSerializer<>(originalSerializer.duplicate());
	}

	@Override
	public T createInstance() {
		singleThreadAccessChecker.checkSingleThreadAccess();
		return originalSerializer.createInstance();
	}

	@Override
	public T copy(T from) {
		singleThreadAccessChecker.checkSingleThreadAccess();
		return originalSerializer.copy(from);
	}

	@Override
	public T copy(T from, T reuse) {
		singleThreadAccessChecker.checkSingleThreadAccess();
		return originalSerializer.copy(from, reuse);
	}

	@Override
	public int getLength() {
		singleThreadAccessChecker.checkSingleThreadAccess();
		return originalSerializer.getLength();
	}

	@Override
	public void serialize(T record, DataOutputView target) throws IOException {
		singleThreadAccessChecker.checkSingleThreadAccess();
		originalSerializer.serialize(record, target);
	}

	@Override
	public T deserialize(DataInputView source) throws IOException {
		singleThreadAccessChecker.checkSingleThreadAccess();
		return originalSerializer.deserialize(source);
	}

	@Override
	public T deserialize(T reuse, DataInputView source) throws IOException {
		singleThreadAccessChecker.checkSingleThreadAccess();
		return originalSerializer.deserialize(reuse, source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		singleThreadAccessChecker.checkSingleThreadAccess();
		originalSerializer.copy(source, target);
	}

	@Override
	public boolean equals(Object obj) {
		singleThreadAccessChecker.checkSingleThreadAccess();
		return obj == this ||
			(obj != null && obj.getClass() == getClass() &&
				originalSerializer.equals(obj));
	}

	@Override
	public int hashCode() {
		singleThreadAccessChecker.checkSingleThreadAccess();
		return originalSerializer.hashCode();
	}

	@Override
	public TypeSerializerSnapshot<T> snapshotConfiguration() {
		singleThreadAccessChecker.checkSingleThreadAccess();
		return originalSerializer.snapshotConfiguration();
	}

	public static class SingleThreadAccessChecker implements Serializable {
		private static final long serialVersionUID = 131020282727167064L;

		private transient Thread currentThread = null;

		public void checkSingleThreadAccess() {
			if (currentThread == null) {
				currentThread = Thread.currentThread();
			} else {
				Preconditions.checkArgument(
					Thread.currentThread().equals(currentThread), "Concurrent access from another thread");
			}
		}
	}
}
