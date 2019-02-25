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

package org.apache.flink.api.scala.typeutils;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import scala.collection.TraversableOnce;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link org.apache.flink.api.common.typeutils.TypeSerializerSnapshot} for the Scala
 * {@link TraversableSerializer}.
 *
 * <p>This configuration snapshot class is implemented in Java because Scala does not
 * allow calling different base class constructors from subclasses, while we need that
 * for the default empty constructor.
 */
public class TraversableSerializerSnapshot<T extends TraversableOnce<E>, E>
		extends CompositeTypeSerializerSnapshot<T, TraversableSerializer<T, E>> {

	private static final int VERSION = 2;

	private String cbfCode;

	@SuppressWarnings("unused")
	public TraversableSerializerSnapshot() {
		super(TraversableSerializer.class);
	}

	public TraversableSerializerSnapshot(TraversableSerializer<T, E> serializerInstance) {
		super(serializerInstance);
		this.cbfCode = serializerInstance.cbfCode();
	}

	TraversableSerializerSnapshot(String cbfCode) {
		super(TraversableSerializer.class);
		checkArgument(cbfCode != null, "cbfCode cannot be null");

		this.cbfCode = cbfCode;
	}

	@Override
	protected int getCurrentOuterSnapshotVersion() {
		return VERSION;
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(
			TraversableSerializer<T, E> outerSerializer) {
		return new TypeSerializer[]{outerSerializer.elementSerializer()};
	}

	@Override
	@SuppressWarnings({"unchecked"})
	protected TraversableSerializer<T, E> createOuterSerializerWithNestedSerializers(
			TypeSerializer<?>[] nestedSerializers) {
		checkState(cbfCode != null,
				"cbfCode cannot be null");

		TypeSerializer<E> nestedSerializer = (TypeSerializer<E>) nestedSerializers[0];
		return new TraversableSerializer<>(nestedSerializer, cbfCode);
	}

	@Override
	protected void writeOuterSnapshot(DataOutputView out) throws IOException {
		out.writeUTF(cbfCode);
	}

	@Override
	protected void readOuterSnapshot(
			int readOuterSnapshotVersion,
			DataInputView in,
			ClassLoader userCodeClassLoader) throws IOException {
		cbfCode = in.readUTF();
	}

	@Override
	protected boolean isOuterSnapshotCompatible(TraversableSerializer<T, E> newSerializer) {
		return cbfCode.equals(newSerializer.cbfCode());
	}
}
