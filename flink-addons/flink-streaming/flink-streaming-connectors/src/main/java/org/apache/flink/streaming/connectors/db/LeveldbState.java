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
import static org.fusesource.leveldbjni.JniDBFactory.bytes;
import static org.fusesource.leveldbjni.JniDBFactory.factory;

import java.io.File;
import java.io.IOException;

import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

public class LeveldbState implements DBState{

	private DB database;

	public LeveldbState(String dbName) {
		Options options = new Options();
		options.createIfMissing(true);
		try {
			database = factory.open(new File(dbName), options);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void close() {
		try {
			database.close();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public void put(String key, String value) {
		database.put(bytes(key), bytes(value));
	}

	public String get(String key) {
		return asString(database.get(bytes(key)));
	}

	public void remove(String key) {
		database.delete(bytes(key));
	}

	public LeveldbStateIterator getIterator() {
		return new LeveldbStateIterator(database.iterator());
	}

}
