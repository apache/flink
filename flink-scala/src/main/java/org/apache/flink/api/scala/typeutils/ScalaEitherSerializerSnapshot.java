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

import org.apache.flink.api.common.typeutils.CompositeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

import scala.util.Either;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Configuration snapshot for serializers of Scala's {@link Either} type,
 * containing configuration snapshots of the Left and Right serializers.
 */
public class ScalaEitherSerializerSnapshot<L, R> implements TypeSerializerSnapshot<Either<L, R>> {

	private static final int CURRENT_VERSION = 1;

	private CompositeSerializerSnapshot nestedLeftRightSerializerSnapshot;

	/**
	 * Constructor for read instantiation.
	 */
	public ScalaEitherSerializerSnapshot() {}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public ScalaEitherSerializerSnapshot(TypeSerializer<L> leftSerializer, TypeSerializer<R> rightSerializer) {
		Preconditions.checkNotNull(leftSerializer);
		Preconditions.checkNotNull(rightSerializer);
		this.nestedLeftRightSerializerSnapshot = new CompositeSerializerSnapshot(leftSerializer, rightSerializer);
	}

	@Override
	public int getCurrentVersion() {
		return CURRENT_VERSION;
	}

	@Override
	public TypeSerializer<Either<L, R>> restoreSerializer() {
		return new EitherSerializer<>(
			nestedLeftRightSerializerSnapshot.getRestoreSerializer(0),
			nestedLeftRightSerializerSnapshot.getRestoreSerializer(1));
	}

	@Override
	public TypeSerializerSchemaCompatibility<Either<L, R>> resolveSchemaCompatibility(
			TypeSerializer<Either<L, R>> newSerializer) {
		checkState(nestedLeftRightSerializerSnapshot != null);

		if (newSerializer instanceof EitherSerializer) {
			EitherSerializer<L, R> serializer = (EitherSerializer<L, R>) newSerializer;

			return nestedLeftRightSerializerSnapshot.resolveCompatibilityWithNested(
				TypeSerializerSchemaCompatibility.compatibleAsIs(),
				serializer.getLeftSerializer(),
				serializer.getRightSerializer());
		}
		else {
			return TypeSerializerSchemaCompatibility.incompatible();
		}
	}

	@Override
	public void writeSnapshot(DataOutputView out) throws IOException {
		nestedLeftRightSerializerSnapshot.writeCompositeSnapshot(out);
	}

	@Override
	public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
		this.nestedLeftRightSerializerSnapshot = CompositeSerializerSnapshot.readCompositeSnapshot(in, userCodeClassLoader);
	}
}
