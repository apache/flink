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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Utility methods for dealing with Protobuf types. This has a default implementation for the case that
 * Avro is not present on the classpath and an actual implementation in flink-avro that is
 * dynamically loaded when present.
 */
public abstract class ProtobufUtils {

	private static final String PROTOBUF_SERIALIZER_UTILS = "org.apache.flink.formats.protobuf.utils.ProtobufSerializerUtils";

	/**
	 * Returns either the default {@link AvroUtils} which throw an exception in cases where Avro
	 * would be needed or loads the specific utils for Avro from flink-avro.
	 */
	public static ProtobufUtils getProtobufUtils() {
		// try and load the special AvroUtils from the flink-avro package
		try {
			Class<?> clazz = Class.forName(PROTOBUF_SERIALIZER_UTILS, false, Thread.currentThread().getContextClassLoader());
			return clazz.asSubclass(ProtobufUtils.class).getConstructor().newInstance();
		} catch (ClassNotFoundException e) {
			// cannot find the utils, return the default implementation
			return new ProtobufUtils.DefaultProtobufUtils();
		} catch (Exception e) {
			throw new RuntimeException("Could not instantiate " + PROTOBUF_SERIALIZER_UTILS + ".", e);
		}
	}

	/**
	 * Creates an {@code AvroTypeInfo} if flink-protobuf is present, otherwise throws an exception.
	 */
	public abstract <T> TypeInformation<T> createProtobufTypeInfo(Class<T> type);

	// ------------------------------------------------------------------------

	/**
	 * A default implementation of the AvroUtils used in the absence of Avro.
	 */
	private static class DefaultProtobufUtils extends ProtobufUtils {

		@Override
		public <T> TypeInformation<T> createProtobufTypeInfo(Class<T> type) {
			throw new RuntimeException("Could not load the ProtobufTypeInfo class. " +
				"You may be missing the 'flink-protobuf' dependency.");
		}
	}
}
