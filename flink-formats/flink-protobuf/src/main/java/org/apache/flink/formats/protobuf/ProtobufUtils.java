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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Utilities to work with protobuf classes.
 */
public class ProtobufUtils {

	private static final Logger LOG = LoggerFactory.getLogger(ProtobufUtils.class);
	private static final int PROTO_CLASS_CACHE_SIZE = 1_000;

	private static LoadingCache<Class<? extends Message>, Descriptor> protoMessageDescriptorCache =
		CacheBuilder.newBuilder()
			.maximumSize(PROTO_CLASS_CACHE_SIZE)
			.build(
				new CacheLoader<Class<? extends Message>, Descriptor>() {
					public Descriptor load(Class<? extends Message> protoClass) {
						return getMessageDescriptorUncached(protoClass);
					}
				});

	private static LoadingCache<Class<? extends Message>, Method> protoBuilderMethodCache =
		CacheBuilder.newBuilder()
			.maximumSize(PROTO_CLASS_CACHE_SIZE)
			.build(
				new CacheLoader<Class<? extends Message>, Method>() {
					public Method load(Class<? extends Message> key) {
						return getBuilderMethodUncached(key);
					}
				});

	private static Descriptor getMessageDescriptorUncached(Class<? extends Message> protoClass) {
		try {
			Method getDescriptor = protoClass.getMethod("getDescriptor", new Class[]{});
			return (Descriptor) getDescriptor.invoke(null, new Object[]{});
		} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
			LOG.error("Could not get method 'getDescriptor' in protobuf class " + protoClass, e);
			throw new IllegalArgumentException(e);
		}
	}

	public static Descriptor getMessageDescriptor(Class<? extends Message> protoClass) {
		try {
			return protoMessageDescriptorCache.get(protoClass);
		} catch (ExecutionException e) {
			LOG.error("Error getting method 'getDescriptor' in 'protobuf' class " + protoClass, e);
			throw new IllegalArgumentException(e);
		}
	}

	private static String firstLetterToLowerCase(String s) {
		if (s == null || s.length() == 0) {
			return s;
		} else {
			return s.substring(0, 1).toLowerCase() + s.substring(1);
		}
	}

	/**
	 * Given a proto field name provide the flink exposed field name.
	 */
	public static Field getField(
		Class<? extends Message> protoClass, FieldDescriptor fieldDescriptor) {
		// Generated Java field names ends with _, like: age_ and the getter name is: getAge()
		// The rules around casing are... tricky. Most fields aren't capitalized, even if
		// the protobuf Field is, e.g., Id -> id_. However, if the field starts with underscores,
		// the fields *will* be capitalized, e.g., __metadata__ -> Metadata_.
		List<Field> fields =
			Arrays.stream(protoClass.getDeclaredFields())
				.filter(f -> f.getName().equalsIgnoreCase(fieldDescriptor.getJsonName() + "_"))
				.collect(Collectors.toList());
		if (fields.isEmpty()) {
			throw new IllegalArgumentException(
				String.format("Could not find declared field '%s'", fieldDescriptor.getName()));
		} else if (fields.size() > 1) {
			throw new IllegalArgumentException(
				String.format(
					"Found multiple Java fields matching proto field '%s': %s",
					fieldDescriptor.getName(),
					StringUtils.join(
						fields.stream().map(f -> f.getName()).collect(Collectors.toList()), ", ")));
		} else {
			return fields.get(0);
		}
	}

	private static Method getBuilderMethodUncached(Class<? extends Message> protoClass) {
		try {
			return protoClass.getMethod("newBuilder");
		} catch (NoSuchMethodException e) {
			LOG.error("Could not find method newBuilder in class " + protoClass, e);
			throw new IllegalArgumentException(e);
		}
	}

	public static Builder getNewBuilder(Class<? extends Message> protoClass) {
		try {
			return (Builder) protoBuilderMethodCache.get(protoClass).invoke(null);
		} catch (IllegalAccessException | ExecutionException | InvocationTargetException e) {
			LOG.error("Could not execute method 'newBuilder' in proto class " + protoClass, e);
			throw new IllegalArgumentException(e);
		}
	}
}
