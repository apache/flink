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

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;

/**
 * A serialized factory who is only able to create in memory serializers and can not be serialized and de-serialized.
 */
public class DuplicateOnlySerializerFactory<T> implements TypeSerializerFactory<T> {
	private final TypeSerializer<T> serializer;
	private boolean firstSerializer = true;

	public DuplicateOnlySerializerFactory(TypeSerializer<T> serializer) {
		this.serializer = serializer;
	}

	@Override
	public TypeSerializer<T> getSerializer() {
		if (firstSerializer) {
			firstSerializer = false;
			return serializer;
		}

		return serializer.duplicate();
	}

	@Override
	public void writeParametersToConfig(Configuration config) {
		throw new UnsupportedOperationException("Writing is not supported in DuplicateOnlySerializerFactory");
	}

	@Override
	public void readParametersFromConfig(Configuration config, ClassLoader cl) throws ClassNotFoundException {
		throw new UnsupportedOperationException("Reading is not supported in DuplicateOnlySerializerFactory");
	}

	@Override
	public Class<T> getDataType() {
		throw new UnsupportedOperationException("Getting data type is not supported in DuplicateOnlySerializerFactory");
	}
}
