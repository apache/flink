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

import scala.util.Either;

/**
 * Configuration snapshot for serializers of Scala's {@link Either} type,
 * containing configuration snapshots of the Left and Right serializers.
 */
public class ScalaEitherSerializerSnapshot<L, R> extends CompositeTypeSerializerSnapshot<Either<L, R>, EitherSerializer<L, R>> {

	private static final int CURRENT_VERSION = 1;

	/**
	 * Constructor for read instantiation.
	 */
	public ScalaEitherSerializerSnapshot() {
		super(EitherSerializer.class);
	}

	/**
	 * Constructor to create the snapshot for writing.
	 */
	public ScalaEitherSerializerSnapshot(EitherSerializer<L, R> eitherSerializer) {
		super(eitherSerializer);
	}

	@Override
	public int getCurrentOuterSnapshotVersion() {
		return CURRENT_VERSION;
	}

	@Override
	protected EitherSerializer<L, R> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		@SuppressWarnings("unchecked")
		TypeSerializer<L> leftSerializer = (TypeSerializer<L>) nestedSerializers[0];

		@SuppressWarnings("unchecked")
		TypeSerializer<R> rightSerializer = (TypeSerializer<R>) nestedSerializers[1];

		return new EitherSerializer<>(leftSerializer, rightSerializer);
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(EitherSerializer<L, R> outerSerializer) {
		return new TypeSerializer<?>[] { outerSerializer.getLeftSerializer(), outerSerializer.getRightSerializer() };
	}
}
