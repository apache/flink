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

package org.apache.flink.runtime.io.async.iotasks;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.io.async.AbstractAsyncIOCallable;
import org.apache.flink.util.InstantiationUtil;

import java.io.InputStream;
import java.io.Serializable;

public class AsyncInputStreamDeserialization<T extends Serializable> extends AbstractAsyncIOCallable<T, InputStream> {

	private final TypeSerializer<T> serializer;

	public AsyncInputStreamDeserialization(InputStream ioHandle, TypeSerializer<T> serializer) {
		super(ioHandle);
		this.serializer = serializer;
	}

	@Override
	public T call() throws Exception {
		return serializer != null ?
				serializer.deserialize(new DataInputViewStreamWrapper(ioHandle)) :
				InstantiationUtil.<T>deserializeObject(ioHandle);
	}
}