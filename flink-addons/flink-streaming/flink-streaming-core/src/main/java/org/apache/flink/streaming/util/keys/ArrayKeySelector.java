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

package org.apache.flink.streaming.util.keys;

import java.lang.reflect.Array;

import org.apache.flink.api.java.tuple.Tuple;

public class ArrayKeySelector<IN> extends FieldsKeySelector<IN> {

	private static final long serialVersionUID = 1L;

	public ArrayKeySelector(int... fields) {
		super(fields);
	}

	@Override
	public Object getKey(IN value) throws Exception {
		if (simpleKey) {
			return Array.get(value, keyFields[0]);
		} else {
			int c = 0;
			for (int pos : keyFields) {
				((Tuple) key).setField(Array.get(value, pos), c);
				c++;
			}
			return key;
		}
	}
}
