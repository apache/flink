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

package org.apache.flink.tez.runtime.input;


import org.apache.flink.util.MutableObjectIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.tez.runtime.library.api.KeyValueReader;

import java.io.IOException;

public class TezReaderIterator<T> implements MutableObjectIterator<T>{

	private KeyValueReader kvReader;

	public TezReaderIterator(KeyValueReader kvReader) {
		this.kvReader = kvReader;
	}

	@Override
	public T next(T reuse) throws IOException {
		if (kvReader.next()) {
			Object key = kvReader.getCurrentKey();
			Object value = kvReader.getCurrentValue();
			if (!(key instanceof IntWritable)) {
				throw new IllegalStateException("Wrong key type");
			}
			reuse = (T) value;
			return reuse;
		}
		else {
			return null;
		}
	}

	@Override
	public T next() throws IOException {
		if (kvReader.next()) {
			Object key = kvReader.getCurrentKey();
			Object value = kvReader.getCurrentValue();
			if (!(key instanceof IntWritable)) {
				throw new IllegalStateException("Wrong key type");
			}
			return (T) value;
		}
		else {
			return null;
		}
	}
}
