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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * Snapshot class for the {@link TimerSerializer}.
 */
@Internal
public class TimerSerializerSnapshot<K, N> extends CompositeTypeSerializerSnapshot<TimerHeapInternalTimer<K, N>, TimerSerializer<K, N>> {

	private static final int VERSION = 2;

	public TimerSerializerSnapshot() {
		super(TimerSerializer.class);
	}

	public TimerSerializerSnapshot(TimerSerializer<K, N> timerSerializer) {
		super(timerSerializer);
	}

	@Override
	protected int getCurrentOuterSnapshotVersion() {
		return VERSION;
	}

	@Override
	protected TimerSerializer<K, N> createOuterSerializerWithNestedSerializers(TypeSerializer<?>[] nestedSerializers) {
		@SuppressWarnings("unchecked")
		final TypeSerializer<K> keySerializer = (TypeSerializer<K>) nestedSerializers[0];

		@SuppressWarnings("unchecked")
		final TypeSerializer<N> namespaceSerializer = (TypeSerializer<N>) nestedSerializers[1];

		return new TimerSerializer<>(keySerializer, namespaceSerializer);
	}

	@Override
	protected TypeSerializer<?>[] getNestedSerializers(TimerSerializer<K, N> outerSerializer) {
		return new TypeSerializer<?>[] { outerSerializer.getKeySerializer(), outerSerializer.getNamespaceSerializer() };
	}
}
