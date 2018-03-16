/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.plugable;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * A {@link DeserializationDelegate} that always creates a new instance upon deserialization.
 */
public class NonReusingDeserializationDelegate<T> implements DeserializationDelegate<T> {

	private T instance;

	private final TypeSerializer<T> serializer;

	public NonReusingDeserializationDelegate(TypeSerializer<T> serializer) {
		this.serializer = serializer;
	}

	public void setInstance(T instance) {
		this.instance = instance;
	}

	public T getInstance() {
		return instance;
	}

	@Override
	public void write(DataOutputView out) throws IOException {
		throw new IllegalStateException("Serialization method called on DeserializationDelegate.");
	}

	@Override
	public void read(DataInputView in) throws IOException {
		this.instance = this.serializer.deserialize(in);
	}
}
