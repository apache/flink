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
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;

import scala.util.Try;

/**
 * A {@link TypeSerializerSnapshot} for the Scala {@link TrySerializer}.
 *
 * <p>This configuration snapshot class is implemented in Java because Scala does not
 * allow calling different base class constructors from subclasses, while we need that
 * for the default empty constructor.
 */
public class ScalaTrySerializerSnapshot<E> extends CompositeTypeSerializerSnapshot<Try<E>, TrySerializer<E>> {

	private static final int VERSION = 2;

	/** This empty nullary constructor is required for deserializing the configuration. */
	@SuppressWarnings("unused")
	public ScalaTrySerializerSnapshot() {
		super(correspondingSerializerClass());
	}

	public ScalaTrySerializerSnapshot(TrySerializer<E> trySerializer) {
		super(trySerializer);
	}

	@Override
	protected int getCurrentOuterSnapshotVersion() {
		return VERSION;
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(TrySerializer<E> outerSerializer) {
		return new TypeSerializer[]{outerSerializer.getElementSerializer(), outerSerializer.getThrowableSerializer()};
	}

	@Override
	@SuppressWarnings("unchecked")
	protected TrySerializer<E> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		TypeSerializer<E> elementSerializer = (TypeSerializer<E>) nestedSerializers[0];
		TypeSerializer<Throwable> valueSerializer = (TypeSerializer<Throwable>) nestedSerializers[1];

		return new TrySerializer<>(elementSerializer, valueSerializer);
	}

	@SuppressWarnings("unchecked")
	private static <E> Class<TrySerializer<E>> correspondingSerializerClass() {
		return (Class<TrySerializer<E>>) (Class<?>) TrySerializer.class;
	}
}
