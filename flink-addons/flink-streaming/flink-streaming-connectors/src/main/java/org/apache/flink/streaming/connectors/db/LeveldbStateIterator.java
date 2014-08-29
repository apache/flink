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

import static org.fusesource.leveldbjni.JniDBFactory.asString;

import org.iq80.leveldb.DBIterator;

public class LeveldbStateIterator implements DBStateIterator {
	private DBIterator iterator;

	public LeveldbStateIterator(DBIterator iter) {
		this.iterator = iter;
		this.iterator.seekToFirst();
	}

	public boolean hasNext() {
		return iterator.hasNext();
	}

	public String getNextKey() {
		return asString(iterator.peekNext().getKey());
	}

	public String getNextValue() {
		return asString(iterator.peekNext().getValue());
	}

	public void next() {
		iterator.next();
	}
}
