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

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateIterator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

abstract class RocksDBStateIterator<K, S extends State, N> implements StateIterator<K, S> {

	private final RocksDBStateBackend backend;
	private final WriteOptions writeOptions;
	private final ColumnFamilyHandle columnFamily;

	private final byte[] namespaceBytes;
	protected final RocksIterator rocksIterator;

	private boolean first;
	private K key;
	private byte[] compoundKey;

	public RocksDBStateIterator(RocksDBStateBackend backend,
			WriteOptions writeOptions,
			ColumnFamilyHandle columnFamily,
			N namespace,
			TypeSerializer<N> namespaceSerializer) throws IOException {
		this.backend = backend;
		this.writeOptions = writeOptions;
		this.columnFamily = columnFamily;
		first = true;
		key = null;

		rocksIterator = backend.db.newIterator(columnFamily);

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
		namespaceSerializer.serialize(namespace, out);

		namespaceBytes = baos.toByteArray();

		out.close();
		baos.close();
	}

	@Override
	public K key() {
		return key;
	}

	@Override
	public void delete() throws Exception {
		backend.db.remove(columnFamily, writeOptions, compoundKey);
	}


	@Override
	public boolean advance() throws Exception {
		if (first) {
			first = false;
			rocksIterator.seek(namespaceBytes);
		} else {
			rocksIterator.next();
		}

		if (!rocksIterator.isValid()) {
			return false;
		}

		compoundKey = rocksIterator.key();
		if (!startsWith(compoundKey, namespaceBytes)) {
			return false;
		}

		DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(new ByteArrayInputStream(compoundKey));
		in.skipBytesToRead(namespaceBytes.length);
		in.readByte();
		key = (K) backend.keySerializer().deserialize(in);
		in.close();

		return true;
	}

	private static boolean startsWith(byte[] source, byte[] match) {

		if (match.length > (source.length)) {
			return false;
		}

		for (int i = 0; i < match.length; i++) {
			if (source[i] != match[i]) {
				return false;
			}
		}
		return true;
	}
}
