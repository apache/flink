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
import org.apache.flink.api.common.typeutils.CompositeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.Either;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Configuration snapshot for the {@link EitherSerializer}.
 */
@Internal
public final class EitherSerializerSnapshot<L, R>
		extends CompositeSerializerSnapshot<Either<L, R>, EitherSerializer<L, R>> {

	private static final int VERSION = 2;

	/**
	 * This empty nullary constructor is required for deserializing the configuration.
	 */
	@SuppressWarnings("unused")
	public EitherSerializerSnapshot() {}

	public EitherSerializerSnapshot(
			TypeSerializer<L> leftSerializer,
			TypeSerializer<R> rightSerializer) {

		super(leftSerializer, rightSerializer);
	}

	// ------------------------------------------------------------------------
	//  Serialization
	// ------------------------------------------------------------------------

	@Override
	public int getCurrentVersion() {
		return VERSION;
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		writeProductSnapshots(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader classLoader) throws IOException {
		switch (readVersion) {
			case 2:
				readProductSnapshots(in, classLoader);
				break;
			default:
				throw new IllegalArgumentException("Unrecognized version: " + readVersion);
		}
	}

	// ------------------------------------------------------------------------
	//  Product Serializer Snapshot Methods
	// ------------------------------------------------------------------------

	@Override
	protected Class<?> outerSerializerType() {
		return EitherSerializer.class;
	}

	@Override
	@SuppressWarnings("unchecked")
	protected TypeSerializer<Either<L, R>> createSerializer(TypeSerializer<?>... nestedSerializers) {
		checkArgument(nestedSerializers.length == 2);

		TypeSerializer<L> left = (TypeSerializer<L>) nestedSerializers[0];
		TypeSerializer<R> right = (TypeSerializer<R>) nestedSerializers[1];

		return new EitherSerializer<>(left, right);
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializersFromSerializer(EitherSerializer<L, R> serializer) {
		return new TypeSerializer[] { serializer.getLeftSerializer(), serializer.getRightSerializer() };
	}

	@Override
	protected TypeSerializerSchemaCompatibility<Either<L, R>, EitherSerializer<L, R>> outerCompatibility(EitherSerializer<L, R> serializer) {
		return TypeSerializerSchemaCompatibility.compatibleAsIs();
	}
}
