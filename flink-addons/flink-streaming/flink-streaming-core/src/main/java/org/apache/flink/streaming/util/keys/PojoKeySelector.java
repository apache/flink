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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.common.typeutils.CompositeType.FlatFieldDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.PojoComparator;

public class PojoKeySelector<IN> extends FieldsKeySelector<IN> {

	private static final long serialVersionUID = 1L;

	PojoComparator<IN> comparator;

	public PojoKeySelector(TypeInformation<IN> type, String... fields) {
		super(new int[removeDuplicates(fields).length]);
		if (!(type instanceof CompositeType<?>)) {
			throw new IllegalArgumentException(
					"Key expressions are only supported on POJO types and Tuples. "
							+ "A type is considered a POJO if all its fields are public, or have both getters and setters defined");
		}
		CompositeType<IN> cType = (CompositeType<IN>) type;

		String[] keyFields = removeDuplicates(fields);
		int numOfKeys = keyFields.length;

		List<FlatFieldDescriptor> fieldDescriptors = new ArrayList<FlatFieldDescriptor>();
		for (String field : keyFields) {
			cType.getKey(field, 0, fieldDescriptors);
		}

		int[] logicalKeyPositions = new int[numOfKeys];
		boolean[] orders = new boolean[numOfKeys];

		for (int i = 0; i < numOfKeys; i++) {
			logicalKeyPositions[i] = fieldDescriptors.get(i).getPosition();
		}

		if (cType instanceof PojoTypeInfo) {
			comparator = (PojoComparator<IN>) cType
					.createComparator(logicalKeyPositions, orders, 0);
		} else {
			throw new IllegalArgumentException(
					"Key expressions are only supported on POJO types. "
							+ "A type is considered a POJO if all its fields are public, or have both getters and setters defined");
		}

	}

	@Override
	public Object getKey(IN value) throws Exception {

		Field[] keyFields = comparator.getKeyFields();
		if (simpleKey) {
			return comparator.accessField(keyFields[0], value);
		} else {
			int c = 0;
			for (Field field : keyFields) {
				((Tuple) key).setField(comparator.accessField(field, value), c);
				c++;
			}
		}
		return key;
	}

	private static String[] removeDuplicates(String[] in) {
		List<String> ret = new LinkedList<String>();
		for (String el : in) {
			if (!ret.contains(el)) {
				ret.add(el);
			}
		}
		return ret.toArray(new String[ret.size()]);
	}
}
