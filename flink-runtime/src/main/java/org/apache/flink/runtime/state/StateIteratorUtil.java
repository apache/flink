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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.types.Pair;

import java.io.IOException;
import java.util.Iterator;

/**
 * Utils for state iterator-like operations.
 */
public class StateIteratorUtil {

	public static <K> Iterator<K> createKeyIterator(
		Iterator<Pair<byte[], byte[]>> innerIterator,
		TypeSerializer<K> keySerializer,
		int stateNameByteLength
	) {
		return new Iterator<K>() {
			@Override
			public boolean hasNext() {
				return innerIterator.hasNext();
			}

			@Override
			public K next() {
				byte[] serializedKey = innerIterator.next().getKey();
				try {
					return StateSerializerUtil.getDeserializedKeyForKeyedValueState(
						serializedKey,
						keySerializer,
						stateNameByteLength);
				} catch (IOException e) {
					throw new StateAccessException(e);
				}
			}

			@Override
			public void remove() {
				innerIterator.remove();
			}
		};
	}
}
