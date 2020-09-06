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

package org.apache.flink.table.runtime.operators.sort;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.disk.iomanager.AbstractChannelReaderInputView;
import org.apache.flink.util.MutableObjectIterator;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

/**
 * Key-Value style channel reader input view iterator.
 */
public class ChannelReaderKVInputViewIterator<K, V> implements MutableObjectIterator<Tuple2<K, V>> {
	private final AbstractChannelReaderInputView inView;
	private final TypeSerializer<K> keySerializer;
	private final TypeSerializer<V> valueSerializer;
	private final List<MemorySegment> freeMemTarget;

	public ChannelReaderKVInputViewIterator(
			AbstractChannelReaderInputView inView,
			List<MemorySegment> freeMemTarget,
			TypeSerializer<K> keySerializer,
			TypeSerializer<V> valueSerializer) {
		this.inView = inView;
		this.freeMemTarget = freeMemTarget;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
	}

	@Override
	public Tuple2<K, V> next(Tuple2<K, V> kvPair) throws IOException {
		try {
			kvPair.f0 = this.keySerializer.deserialize(kvPair.f0, this.inView);
			kvPair.f1 = this.valueSerializer.deserialize(kvPair.f1, this.inView);
			return kvPair;
		} catch (EOFException var4) {
			List<MemorySegment> freeMem = this.inView.close();
			if (this.freeMemTarget != null) {
				this.freeMemTarget.addAll(freeMem);
			}
			return null;
		}
	}

	@Override
	public Tuple2<K, V> next() throws IOException {
		throw new UnsupportedOperationException("not supported.");
	}
}
