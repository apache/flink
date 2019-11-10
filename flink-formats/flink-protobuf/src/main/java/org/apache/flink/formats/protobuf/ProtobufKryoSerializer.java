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

package org.apache.flink.formats.protobuf;

import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheLoader;
import org.apache.flink.shaded.guava18.com.google.common.cache.LoadingCache;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.Message;

import java.lang.reflect.Method;


/**
 * Kryo serializer for protobouf classes.
 */
public class ProtobufKryoSerializer extends Serializer<Message> {

	private static final int PROTO_CLASS_CACHE_SIZE = 2_000;

	private static LoadingCache<Class<Message>, Method> protoParseMethodsCache =
			CacheBuilder.newBuilder()
					.maximumSize(PROTO_CLASS_CACHE_SIZE)
					.build(
							new CacheLoader<Class<Message>, Method>() {
								public Method load(Class<Message> protoClass) {
									try {
										return protoClass.getMethod("parseFrom", new Class[]{byte[].class});
									} catch (NoSuchMethodException e) {
										throw new RuntimeException(
												"Couldn't get parseFrom method for proto class: " + protoClass);
									}
								}
							});

	@Override
	public void write(Kryo kryo, Output output, Message protoObj) {
		byte[] bytes = protoObj.toByteArray();
		output.writeInt(bytes.length, true);
		output.writeBytes(bytes);
	}

	@Override
	public Message read(Kryo kryo, Input input, Class<Message> protoClass) {
		try {
			int size = input.readInt(true);
			byte[] bytes = new byte[size];
			input.readBytes(bytes);
			return (Message) protoParseMethodsCache.get(protoClass).invoke(null, bytes);
		} catch (Exception e) {
			throw new RuntimeException("Could not get parse method for: " + protoClass, e);
		}
	}
}
