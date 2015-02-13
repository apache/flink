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
 * WITHOUStreamRecord<?>WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.invokable.operator.windowing;

import java.io.IOException;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

public final class StreamWindowSerializer<T> extends TypeSerializer<StreamWindow<T>> {

	private static final long serialVersionUID = 1L;

	private final TypeSerializer<T> typeSerializer;
	TypeSerializer<Integer> intSerializer;

	public StreamWindowSerializer(TypeInformation<T> typeInfo, ExecutionConfig conf) {
		this.typeSerializer = typeInfo.createSerializer(conf);
		this.intSerializer = BasicTypeInfo.INT_TYPE_INFO.createSerializer(conf);
	}

	public TypeSerializer<T> getObjectSerializer() {
		return typeSerializer;
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public StreamWindow<T> createInstance() {
		return new StreamWindow<T>(0, 0, 0);
	}

	@Override
	public StreamWindow<T> copy(StreamWindow<T> from) {
		return new StreamWindow<T>(from, typeSerializer);
	}

	@Override
	public StreamWindow<T> copy(StreamWindow<T> from, StreamWindow<T> reuse) {
		reuse.clear();
		reuse.windowID = from.windowID;
		reuse.transformationID = from.transformationID;
		reuse.numberOfParts = from.numberOfParts;

		for (T element : from) {
			reuse.add(typeSerializer.copy(element));
		}
		return reuse;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(StreamWindow<T> value, DataOutputView target) throws IOException {

		intSerializer.serialize(value.windowID, target);
		intSerializer.serialize(value.transformationID, target);
		intSerializer.serialize(value.numberOfParts, target);
		intSerializer.serialize(value.size(), target);

		for (T element : value) {
			typeSerializer.serialize(element, target);
		}
	}

	@Override
	public StreamWindow<T> deserialize(DataInputView source) throws IOException {
		StreamWindow<T> window = createInstance();

		window.windowID = intSerializer.deserialize(source);
		window.transformationID = intSerializer.deserialize(source);
		window.numberOfParts = intSerializer.deserialize(source);

		int size = intSerializer.deserialize(source);

		for (int i = 0; i < size; i++) {
			window.add(typeSerializer.deserialize(source));
		}

		return window;
	}

	@Override
	public StreamWindow<T> deserialize(StreamWindow<T> reuse, DataInputView source)
			throws IOException {

		StreamWindow<T> window = reuse;
		window.clear();

		window.windowID = source.readInt();
		window.transformationID = source.readInt();
		window.numberOfParts = source.readInt();

		int size = source.readInt();

		for (int i = 0; i < size; i++) {
			window.add(typeSerializer.deserialize(source));
		}

		return window;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		// Needs to be implemented
	}

	@Override
	public TypeSerializer<StreamWindow<T>> duplicate() {
		return this;
	}
}
