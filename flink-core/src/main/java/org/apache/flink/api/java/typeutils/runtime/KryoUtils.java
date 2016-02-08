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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

/**
 * Convenience methods for Kryo
 */
@Internal
public class KryoUtils {

	/**
	 * Tries to copy the given record from using the provided Kryo instance. If this fails, then
	 * the record from is copied by serializing it into a byte buffer and deserializing it from
	 * there.
	 *
	 * @param from Element to copy
	 * @param kryo Kryo instance to use
	 * @param serializer TypeSerializer which is used in case of a Kryo failure
	 * @param <T> Type of the element to be copied
	 * @return Copied element
	 */
	public static <T> T copy(T from, Kryo kryo, TypeSerializer<T> serializer) {
		try {
			return kryo.copy(from);
		} catch (KryoException ke) {
			// Kryo could not copy the object --> try to serialize/deserialize the object
			try {
				byte[] byteArray = InstantiationUtil.serializeToByteArray(serializer, from);

				return InstantiationUtil.deserializeFromByteArray(serializer, byteArray);
			} catch (IOException ioe) {
				throw new RuntimeException("Could not copy object by serializing/deserializing" +
					" it.", ioe);
			}
		}
	}

	/**
	 * Tries to copy the given record from using the provided Kryo instance. If this fails, then
	 * the record from is copied by serializing it into a byte buffer and deserializing it from
	 * there.
	 *
	 * @param from Element to copy
	 * @param reuse Reuse element for the deserialization
	 * @param kryo Kryo instance to use
	 * @param serializer TypeSerializer which is used in case of a Kryo failure
	 * @param <T> Type of the element to be copied
	 * @return Copied element
	 */
	public static <T> T copy(T from, T reuse, Kryo kryo, TypeSerializer<T> serializer) {
		try {
			return kryo.copy(from);
		} catch (KryoException ke) {
			// Kryo could not copy the object --> try to serialize/deserialize the object
			try {
				byte[] byteArray = InstantiationUtil.serializeToByteArray(serializer, from);

				return InstantiationUtil.deserializeFromByteArray(serializer, reuse, byteArray);
			} catch (IOException ioe) {
				throw new RuntimeException("Could not copy object by serializing/deserializing" +
					" it.", ioe);
			}
		}
	}
}
