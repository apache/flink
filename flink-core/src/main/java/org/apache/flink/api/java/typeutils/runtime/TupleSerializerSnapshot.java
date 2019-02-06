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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Snapshot of a tuple serializer's configuration.
 */
@Internal
public final class TupleSerializerSnapshot<T extends Tuple>
	extends CompositeTypeSerializerSnapshot<T, TupleSerializer<T>> {

	private static final int VERSION = 2;

	private Class<T> tupleClass;

	@SuppressWarnings("unused")
	public TupleSerializerSnapshot() {
		super(correspondingSerializerClass());
	}

	TupleSerializerSnapshot(TupleSerializer<T> serializerInstance) {
		super(serializerInstance);
		this.tupleClass = serializerInstance.getTupleClass();
	}

	// for backwards compatibility
	TupleSerializerSnapshot(Class<T> tupleClass) {
		super(correspondingSerializerClass());
		this.tupleClass = tupleClass;
	}

	@Override
	protected int getCurrentOuterSnapshotVersion() {
		return VERSION;
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(TupleSerializer<T> outerSerializer) {
		return outerSerializer.getFieldSerializers();
	}

	@Override
	protected TupleSerializer<T> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		return new TupleSerializer<>(tupleClass, nestedSerializers);
	}

	@Override
	protected void writeOuterSnapshot(DataOutputView out) throws IOException {
		out.writeUTF(tupleClass.getName());
	}

	@Override
	protected void readOuterSnapshot(int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		final String className = in.readUTF();
		try {
			@SuppressWarnings("unchecked")
			Class<T> typeClass = (Class<T>) Class.forName(className, false, userCodeClassLoader);
			this.tupleClass = typeClass;
		}
		catch (ClassNotFoundException e) {
			throw new IllegalStateException("Can not find the tuple class '" + tupleClass + "'", e);
		}
	}

	@SuppressWarnings("unchecked")
	private static <T extends Tuple> Class<TupleSerializer<T>> correspondingSerializerClass() {
		return (Class<TupleSerializer<T>>) (Class<?>) TupleSerializer.class;
	}
}
