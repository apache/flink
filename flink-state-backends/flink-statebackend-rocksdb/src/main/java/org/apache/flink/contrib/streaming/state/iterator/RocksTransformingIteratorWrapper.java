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

package org.apache.flink.contrib.streaming.state.iterator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.core.memory.ByteArrayDataInputView;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.util.FlinkRuntimeException;

import org.rocksdb.RocksIterator;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * Wrapper around {@link RocksIterator} that applies a given {@link StateSnapshotTransformer} to the elements
 * during the iteration.
 *
 * @param <K> type of key
 * @param <N> type of namespace
 */
public class RocksTransformingIteratorWrapper<K, N> extends RocksIteratorWrapper {
	@Nonnull
	private final RocksDBSnapshotTransformContext<K, N, byte[]> transformContext;
	private byte[] current;
	private final ByteArrayDataInputView div;

	public RocksTransformingIteratorWrapper(
		@Nonnull RocksIterator iterator,
		@Nonnull RocksDBSnapshotTransformContext<K, N, byte[]> transformContext) {
		super(iterator);
		this.transformContext = transformContext;
		this.div = new ByteArrayDataInputView();
	}

	@Override
	public void seekToFirst() {
		super.seekToFirst();
		filterOrTransform(super::next);
	}

	@Override
	public void seekToLast() {
		super.seekToLast();
		filterOrTransform(super::prev);
	}

	@Override
	public void next() {
		super.next();
		filterOrTransform(super::next);
	}

	@Override
	public void prev() {
		super.prev();
		filterOrTransform(super::prev);
	}

	private void filterOrTransform(Runnable advance) {
		while (isValid() && (current = filterNextValue()) == null) {
			advance.run();
		}
	}

	private byte[] filterNextValue() {
		Tuple2<K, N> keyAndNs = deserializeKeyAndNamespaces();
		return transformContext.getTransformer().filterOrTransform(keyAndNs.f0, keyAndNs.f1, super.value());
	}

	private Tuple2<K, N> deserializeKeyAndNamespaces() {
		byte[] rocksdbKey = key();
		int keyGroupPrefixBytes = transformContext.getKeyGroupPrefixBytes();
		div.setData(rocksdbKey, keyGroupPrefixBytes, rocksdbKey.length - keyGroupPrefixBytes);
		return Tuple2.of(readKey(div), readNamespace(div));
	}

	private K readKey(ByteArrayDataInputView dataInput) {
		try {
			return RocksDBKeySerializationUtils.readKey(
				transformContext.getKeySerializer(),
				dataInput,
				transformContext.isAmbiguousKeyPossible());
		} catch (IOException e) {
			throw new FlinkRuntimeException("Unexpected key deserialization failure whiling filtering state", e);
		}
	}

	private N readNamespace(ByteArrayDataInputView dataInput) {
		try {
			return RocksDBKeySerializationUtils.readNamespace(
				transformContext.getNamespaceSerializer(),
				dataInput,
				transformContext.isAmbiguousKeyPossible());
		} catch (IOException e) {
			throw new FlinkRuntimeException("Unexpected namespace deserialization failure whiling filtering state", e);
		}
	}

	@Override
	public byte[] value() {
		if (!isValid()) {
			throw new IllegalStateException("value() method cannot be called if isValid() is false");
		}
		return current;
	}

	/** State context needed by {@link RocksTransformingIteratorWrapper}. */
	public static class RocksDBSnapshotTransformContext<K, N, T> {
		private final int keyGroupPrefixBytes;
		private final TypeSerializer<K> keySerializer;
		private final TypeSerializer<N> namespaceSerializer;
		private final StateSnapshotTransformer<K, N, T> transformer;
		private final boolean ambiguousKeyPossible;

		public RocksDBSnapshotTransformContext(
			int keyGroupPrefixBytes,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			StateSnapshotTransformer<K, N, T> transformer) {
			this.keyGroupPrefixBytes = keyGroupPrefixBytes;
			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
			this.transformer = transformer;
			this.ambiguousKeyPossible =
				RocksDBKeySerializationUtils.isAmbiguousKeyPossible(keySerializer, namespaceSerializer);
		}

		public int getKeyGroupPrefixBytes() {
			return keyGroupPrefixBytes;
		}

		public TypeSerializer<K> getKeySerializer() {
			return keySerializer;
		}

		public TypeSerializer<N> getNamespaceSerializer() {
			return namespaceSerializer;
		}

		StateSnapshotTransformer<K, N, T> getTransformer() {
			return transformer;
		}

		boolean isAmbiguousKeyPossible() {
			return ambiguousKeyPossible;
		}
	}
}
