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

package org.apache.flink.formats.thrift;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.thrift.typeutils.ThriftTypeInfo;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;

import java.io.IOException;

/**
 * ThriftDeserializationSchema that extends DeserializationSchema interface.
 * @param <T> The Thrift class.
 */
public class ThriftDeserializationSchema<T extends TBase> implements DeserializationSchema {

	private Class<T> thriftClazz;
	private ThriftCodeGenerator codeGenerator;

	public ThriftDeserializationSchema(Class<T> recordClazz, ThriftCodeGenerator codeGenerator) {
		this.thriftClazz = recordClazz;
		this.codeGenerator = codeGenerator;
	}

	@Override
	public T deserialize(byte[] message) throws IOException {
		TDeserializer deserializer = new TDeserializer();
		T instance = null;
		try {
			instance = thriftClazz.newInstance();
			deserializer.deserialize(instance, message);
		} catch (Exception e) {

		}
		return instance;
	}

	public boolean isEndOfStream(Object nextElement) {
		return false;
	}

	public TypeInformation<T> getProducedType() {
		return new ThriftTypeInfo<>(thriftClazz, codeGenerator);
	}
}
