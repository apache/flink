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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This accumulator stores a collection of objects in serialized form, so that the stored objects
 * are not affected by modifications to the original objects.
 *
 * Objects may be deserialized on demand with a specific classloader.
 *
 * @param <T> The type of the accumulated objects
 */
@PublicEvolving
public class SerializedListAccumulator<T> implements Accumulator<T, ArrayList<byte[]>> {

	private static final long serialVersionUID = 1L;

	private ArrayList<byte[]> localValue = new ArrayList<>();


	@Override
	public void add(T value) {
		throw new UnsupportedOperationException();
	}

	public void add(T value, TypeSerializer<T> serializer) throws IOException {
		try {
			ByteArrayOutputStream outStream = new ByteArrayOutputStream();
			DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(outStream);
			serializer.serialize(value, out);
			localValue.add(outStream.toByteArray());
		}
		catch (IOException e) {
			throw new IOException("Failed to serialize value '" + value + '\'', e);
		}
	}

	@Override
	public ArrayList<byte[]> getLocalValue() {
		return localValue;
	}

	@Override
	public void resetLocal() {
		localValue.clear();
	}

	@Override
	public void merge(Accumulator<T, ArrayList<byte[]>> other) {
		localValue.addAll(other.getLocalValue());
	}

	@Override
	public SerializedListAccumulator<T> clone() {
		SerializedListAccumulator<T> newInstance = new SerializedListAccumulator<T>();
		newInstance.localValue = new ArrayList<byte[]>(localValue);
		return newInstance;
	}

	@SuppressWarnings("unchecked")
	public static <T> List<T> deserializeList(ArrayList<byte[]> data, TypeSerializer<T> serializer)
			throws IOException, ClassNotFoundException
	{
		List<T> result = new ArrayList<T>(data.size());
		for (byte[] bytes : data) {
			ByteArrayInputStream inStream = new ByteArrayInputStream(bytes);
			DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(inStream);
			T val = serializer.deserialize(in);
			result.add(val);
		}
		return result;
	}

	@Override
	public String toString() {
		return "SerializedListAccumulator: " + localValue.size() + " elements";
	}
}
