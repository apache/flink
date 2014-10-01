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

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;

import redis.clients.jedis.Jedis;

/**
 * Interface to a Redis key-value cache. It needs a running instance of Redis.
 * 
 * @see <a href="http://redis.io/">http://redis.io/</a>
 * 
 * @param <K>
 *            Type of key
 * @param <V>
 *            Type of value
 */
public class RedisState<K extends Serializable, V extends Serializable> extends
		CustomSerializationDBState<K, V> implements DBStateWithIterator<K, V> {

	private Jedis jedis;

	public RedisState(DBSerializer<K> keySerializer, DBSerializer<V> valueSerializer) {
		super(keySerializer, valueSerializer);
		jedis = new Jedis("localhost");
	}

	public RedisState() {
		this(new DefaultDBSerializer<K>(), new DefaultDBSerializer<V>());
	}

	@Override
	public void close() {
		jedis.close();
	}

	@Override
	public void put(K key, V value) {
		jedis.set(keySerializer.write(key), valueSerializer.write(value));
	}

	@Override
	public V get(K key) {
		return valueSerializer.read(jedis.get(keySerializer.write(key)));
	}

	@Override
	public void remove(K key) {
		jedis.del(keySerializer.write(key));
	}

	@Override
	public DBStateIterator<K, V> getIterator() {
		return new RedisStateIterator();
	}

	private class RedisStateIterator extends DBStateIterator<K, V> {

		private Set<byte[]> set;
		private Iterator<byte[]> iterator;
		private byte[] currentKey;

		public RedisStateIterator() {
			set = jedis.keys(new byte[0]);
			jedis.keys("*".getBytes()).iterator();
			iterator = set.iterator();
			currentKey = iterator.next();
		}

		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}

		@Override
		public K getNextKey() {
			return keySerializer.read(currentKey);
		}

		@Override
		@SuppressWarnings("unchecked")
		public V getNextValue() {
			return (V) jedis.get(currentKey);
		}

		@Override
		public void next() {
			currentKey = iterator.next();
		}
	}
}
