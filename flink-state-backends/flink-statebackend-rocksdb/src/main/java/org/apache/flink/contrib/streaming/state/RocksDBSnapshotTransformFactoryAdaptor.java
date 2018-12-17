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
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StateSnapshotTransformer.StateSnapshotTransformFactory;

import java.util.Optional;

abstract class RocksDBSnapshotTransformFactoryAdaptor<S> implements StateSnapshotTransformFactory<S> {
	@Override
	public Optional<StateSnapshotTransformer<S>> createForDeserializedState() {
		throw new UnsupportedOperationException("Only serialized state filtering is supported in RocksDB backend");
	}

	@SuppressWarnings("unchecked")
	static <SV, SEV> StateSnapshotTransformFactory<SV> wrapStateSnapshotTransformerFactory(
		StateDescriptor<?, SV> stateDesc,
		StateSnapshotTransformFactory<SEV> snapshotTransformFactory) {
		if (stateDesc instanceof ListStateDescriptor) {
			Optional<StateSnapshotTransformer<SEV>> original = snapshotTransformFactory.createForDeserializedState();
			return new RocksDBSnapshotTransformFactoryAdaptor<SV>() {
				@Override
				public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
					return original.map(est -> (StateSnapshotTransformer<byte[]>) createRocksDBListStateTransformer(stateDesc, est));
				}
			};
		} else if (stateDesc instanceof MapStateDescriptor) {
			Optional<StateSnapshotTransformer<byte[]>> original = snapshotTransformFactory.createForSerializedState();
			return new RocksDBSnapshotTransformFactoryAdaptor<SV>() {
				@Override
				public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
					return original.map(RocksDBMapState.StateSnapshotTransformerWrapper::new);
				}
			};
		} else {
			return new RocksDBSnapshotTransformFactoryAdaptor<SV>() {
				@Override
				public Optional<StateSnapshotTransformer<byte[]>> createForSerializedState() {
					return snapshotTransformFactory.createForSerializedState();
				}
			};
		}
	}

	@SuppressWarnings("unchecked")
	private static <SV, SEV> StateSnapshotTransformer<SV> createRocksDBListStateTransformer(
		StateDescriptor<?, SV> stateDesc,
		StateSnapshotTransformer<SEV> elementTransformer) {
		return (StateSnapshotTransformer<SV>) new RocksDBListState.StateSnapshotTransformerWrapper<>(
			elementTransformer, ((ListStateDescriptor<SEV>) stateDesc).getElementSerializer());
	}
}
