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

import org.apache.flink.api.common.serialization.SerializationSchema;

import org.apache.thrift.TBase;
import org.apache.thrift.TSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ThriftSerializationSchema that extends SerializationSchema interface.
 *
 * @param <T> The Thrift class.
 */
public class ThriftSerializationSchema<T extends TBase> implements SerializationSchema<T> {

	private static final Logger LOG = LoggerFactory.getLogger(ThriftSerializationSchema.class);
	private TSerializer serializer;

	public ThriftSerializationSchema(Class<T> recordClazz) {
		this.serializer = new TSerializer();
	}

	public byte[] serialize(T element) {
		byte[] message = null;
		TSerializer serializer = new TSerializer();
		try {
			message = serializer.serialize(element);
		} catch (Exception e) {
			LOG.error("Failed to serialize message : {}", element, e);
		}
		return message;
	}
}
