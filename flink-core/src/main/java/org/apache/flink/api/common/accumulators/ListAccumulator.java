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


package org.apache.flink.api.common.accumulators;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;

import org.apache.commons.lang3.SerializationUtils;

/**
 * * This accumulator stores a collection of objects which are immediately serialized to cope with object reuse.
 * * When the objects are requested again, they are deserialized.
 * @param <T> The type of the accumulated objects
 */
public class ListAccumulator<T> implements Accumulator<T, ArrayList<T>> {

	private static final long serialVersionUID = 1L;

	private ArrayList<byte[]> localValue = new ArrayList<byte[]>();
	
	@Override
	public void add(T value) {
		byte[] byteArray = SerializationUtils.serialize((Serializable) value);
		localValue.add(byteArray);
	}

	@Override
	public ArrayList<T> getLocalValue() {
		ArrayList<T> arrList = new ArrayList<T>();
		for (byte[] byteArr : localValue) {
			T item = SerializationUtils.deserialize(byteArr);
			arrList.add(item);
		}
		return arrList;
	}

	@Override
	public void resetLocal() {
		localValue.clear();
	}

	@Override
	public void merge(Accumulator<T, ArrayList<T>> other) {
		localValue.addAll(((ListAccumulator<T>) other).localValue);
	}

	@Override
	public Accumulator<T, ArrayList<T>> clone() {
		ListAccumulator<T> newInstance = new ListAccumulator<T>();
		for (byte[] item : localValue) {
			newInstance.localValue.add(item.clone());
		}
		return newInstance;
	}

	@Override
	public void write(ObjectOutputStream out) throws IOException {
		int numItems = localValue.size();
		out.writeInt(numItems);
		for (byte[] item : localValue) {
			out.writeInt(item.length);
			out.write(item);
		}
	}

	@Override
	public void read(ObjectInputStream in) throws IOException {
		int numItems = in.readInt();
		for (int i = 0; i < numItems; i++) {
			int len = in.readInt();
			byte[] obj = new byte[len];
			in.read(obj);
			localValue.add(obj);
		}
	}

}
