/**
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

import java.io.IOException;
import java.net.InetSocketAddress;

import net.spy.memcached.MemcachedClient;

//Needs running Memcached service
public class MemcachedState implements DBState {

	private MemcachedClient memcached;

	public MemcachedState() {
		try {
			memcached = new MemcachedClient(new InetSocketAddress("localhost",
					11211));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public MemcachedState(String hostname, int portNum) {
		try {
			memcached = new MemcachedClient(new InetSocketAddress(hostname,
					portNum));
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		memcached.shutdown();
	}

	public void put(String key, String value) {
		memcached.set(key, 0, value);
	}

	public String get(String key) {
		return (String) memcached.get(key);
	}

	public void remove(String key) {
		memcached.delete(key);
	}
}
