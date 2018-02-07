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

package org.apache.flink.cep.nfa;

import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * {@link CompositeTypeSerializerConfigSnapshot} for {@link NFAStateSerializer}.
 */
public final class NFAStateSerializerConfigSnapshot<T> extends CompositeTypeSerializerConfigSnapshot {

	private static final int VERSION = 1;

	/** This empty constructor is required for deserializing the configuration. */
	public NFAStateSerializerConfigSnapshot() {}

	public NFAStateSerializerConfigSnapshot(
			TypeSerializer<T> eventSerializer,
			TypeSerializer<SharedBuffer<String, T>> sharedBufferSerializer) {

		super(eventSerializer, sharedBufferSerializer);
	}

	@Override
	public int getVersion() {
		return VERSION;
	}
}
