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
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.io.async.AbstractAsyncIOCallable;
import org.apache.flink.util.InstantiationUtil;

import java.io.OutputStream;
import java.io.Serializable;

public class AsyncInputStreamSerialization<T extends Serializable> extends AbstractAsyncIOCallable<T, OutputStream> {

	private final TypeSerializer<T> serializer;
	private final T objectToSerialize;

	public AsyncInputStreamSerialization(OutputStream ioHandle, TypeSerializer<T> serializer, T objectToSerialize) {
		super(ioHandle);
		this.serializer = serializer;
		this.objectToSerialize = objectToSerialize;
	}

	@Override
	public T call() throws Exception {

		if (serializer != null) {
			serializer.serialize(objectToSerialize, new DataOutputViewStreamWrapper(ioHandle));
		} else {
			InstantiationUtil.serializeObject(ioHandle);
		}

		return null;
	}
}