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

package org.apache.flink.connectors.hive;

import org.apache.flink.util.SerializedValue;

import java.io.IOException;

/**
 * An extension of SerializedValue which caches the deserialized data.
 */
public class CachedSerializedValue<T> extends SerializedValue<T> {

	private static final long serialVersionUID = 1L;

	private transient T deserialized;

	public CachedSerializedValue(T value) throws IOException {
		super(value);
	}

	@Override
	public T deserializeValue(ClassLoader loader) throws IOException, ClassNotFoundException {
		if (deserialized == null) {
			deserialized = super.deserializeValue(loader);
		}
		return deserialized;
	}

	public T deserializeValue() throws IOException, ClassNotFoundException {
		return deserializeValue(Thread.currentThread().getContextClassLoader());
	}
}
