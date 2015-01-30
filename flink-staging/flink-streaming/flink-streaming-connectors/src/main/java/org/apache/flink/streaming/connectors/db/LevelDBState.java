/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.db;

import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.Options;

/**
 * Interface to a LevelDB key-value store.
 * 
 * @see <a href="https://code.google.com/p/leveldb/">https://code.google.com/p/leveldb/</a>
 * 
 * @param <K>
 *            Type of key
 * @param <V>
 *            Type of value
 */
public class LevelDBState<K extends Serializable, V extends Serializable> extends
		CustomSerializationDBState<K, V> implements DBStateWithIterator<K, V> {

	private DB database;

	public LevelDBState(String dbName, DBSerializer<K> keySerializer,
			DBSerializer<V> valueSerializer) {
		super(keySerializer, valueSerializer);
		Options options = new Options();
		File file = new File(dbName);
		options.createIfMissing(true);
		try {
			factory.destroy(file, options);
			database = factory.open(file, options);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public LevelDBState(String dbName) {
		this(dbName, new DefaultDBSerializer<K>(), new DefaultDBSerializer<V>());
	}
	
	@Override
	public void close() {
		try {
			database.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void put(K key, V value) {
		database.put(keySerializer.write(key), valueSerializer.write(value));
	}

	@Override
	public V get(K key) {
		byte[] serializedValue = database.get(keySerializer.write(key));
		if (serializedValue != null) {
			return valueSerializer.read(serializedValue);
		} else {
			throw new RuntimeException("No such entry at key " + key);
		}
	}

	@Override
	public void remove(K key) {
		database.delete(keySerializer.write(key));
	}

	@Override
	public DBStateIterator<K, V> getIterator() {
		return new LevelDBStateIterator();
	}

	private class LevelDBStateIterator extends DBStateIterator<K, V> {
		private DBIterator iterator;

		public LevelDBStateIterator() {
			this.iterator = database.iterator();
			this.iterator.seekToFirst();
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public K getNextKey() {
			return keySerializer.read(iterator.peekNext().getKey());
		}

		@Override
		public V getNextValue() {
			return valueSerializer.read(iterator.peekNext().getValue());
		}

		@Override
		public void next() {
			iterator.next();
		}
	}
}
