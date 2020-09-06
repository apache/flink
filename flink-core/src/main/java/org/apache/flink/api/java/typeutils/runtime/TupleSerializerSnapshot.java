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
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

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
		super(TupleSerializer.class);
	}

	TupleSerializerSnapshot(TupleSerializer<T> serializerInstance) {
		super(serializerInstance);
		this.tupleClass = checkNotNull(serializerInstance.getTupleClass(), "tuple class can not be NULL");
	}

	/**
	 * Constructor for backwards compatibility, used by
	 * {@link TupleSerializer#resolveSchemaCompatibilityViaRedirectingToNewSnapshotClass}.
	 */
	TupleSerializerSnapshot(Class<T> tupleClass) {
		super(TupleSerializer.class);
		this.tupleClass = checkNotNull(tupleClass, "tuple class can not be NULL");
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
		checkState(tupleClass != null, "tuple class can not be NULL");
		return new TupleSerializer<>(tupleClass, nestedSerializers);
	}

	@Override
	protected void writeOuterSnapshot(DataOutputView out) throws IOException {
		checkState(tupleClass != null, "tuple class can not be NULL");
		out.writeUTF(tupleClass.getName());
	}

	@Override
	protected void readOuterSnapshot(int readOuterSnapshotVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		this.tupleClass = InstantiationUtil.resolveClassByName(in, userCodeClassLoader);
	}
}
