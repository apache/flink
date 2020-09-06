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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;

import java.util.Optional;

abstract class RocksDBSnapshotTransformFactoryAdaptor<SV, SEV> implements StateSnapshotTransformFactory<SV> {
	final StateSnapshotTransformFactory<SEV> snapshotTransformFactory;

	RocksDBSnapshotTransformFactoryAdaptor(StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
		this.snapshotTransformFactory = snapshotTransformFactory;
	}

	@Override
	public Optional<StateSnapshotTransformer<SV>> createForDeserializedState() {
		throw new UnsupportedOperationException("Only serialized state filtering is supported in RocksDB backend");
	}

	@SuppressWarnings("unchecked")
	static <SV, SEV> StateSnapshotTransformFactory<SV> wrapStateSnapshotTransformFactory(
		StateDescriptor<?, SV> stateDesc,
		StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
		TypeSerializer<SV> stateSerializer) {
		if (stateDesc instanceof ListStateDescriptor) {
			TypeSerializer<SEV> elementSerializer = ((ListSerializer<SEV>) stateSerializer).getElementSerializer();
			return new RocksDBListStateSnapshotTransformFactory<>(snapshotTransformFactory, elementSerializer);
		} else if (stateDesc instanceof MapStateDescriptor) {
			return new RocksDBMapStateSnapshotTransformFactory<>(snapshotTransformFactory);
		} else {
			return new RocksDBValueStateSnapshotTransformFactory<>(snapshotTransformFactory);
		}
	}

	private static class RocksDBValueStateSnapshotTransformFactory<SV, SEV>
		extends RocksDBSnapshotTransformFactoryAdaptor<SV, SEV> {

		private RocksDBValueStateSnapshotTransformFactory(StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
			super(snapshotTransformFactory);
		}

		@Override
		public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
			return snapshotTransformFactory.createForSerializedState();
		}
	}

	private static class RocksDBMapStateSnapshotTransformFactory<SV, SEV>
		extends RocksDBSnapshotTransformFactoryAdaptor<SV, SEV> {

		private RocksDBMapStateSnapshotTransformFactory(StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
			super(snapshotTransformFactory);
		}

		@Override
		public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
			return snapshotTransformFactory.createForSerializedState()
				.map(RocksDBMapState.StateSnapshotTransformerWrapper::new);
		}
	}

	private static class RocksDBListStateSnapshotTransformFactory<SV, SEV>
		extends RocksDBSnapshotTransformFactoryAdaptor<SV, SEV> {

		private final TypeSerializer<SEV> elementSerializer;

		@SuppressWarnings("unchecked")
		private RocksDBListStateSnapshotTransformFactory(
			StateSnapshotTransformFactory<SEV> snapshotTransformFactory,
			TypeSerializer<SEV> elementSerializer) {

			super(snapshotTransformFactory);
			this.elementSerializer = elementSerializer;
		}

		@Override
		public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
			return snapshotTransformFactory.createForDeserializedState()
				.map(est -> new RocksDBListState.StateSnapshotTransformerWrapper<>(est, elementSerializer.duplicate()));
		}
	}
}
