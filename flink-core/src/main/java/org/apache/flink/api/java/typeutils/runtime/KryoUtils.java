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
import com.esotericsoftware.kryo.Serializer;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Collection;

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

	/**
	 * Apply a list of {@link KryoRegistration} to a Kryo instance. The list of registrations is
	 * assumed to already be a final resolution of all possible registration overwrites.
	 *
	 * <p>The registrations are applied in the given order and always specify the registration id as
	 * the next available id in the Kryo instance (providing the id just extra ensures nothing is
	 * overwritten, and isn't strictly required);
	 *
	 * @param kryo the Kryo instance to apply the registrations
	 * @param resolvedRegistrations the registrations, which should already be resolved of all possible registration overwrites
	 */
	public static void applyRegistrations(Kryo kryo, Collection<KryoRegistration> resolvedRegistrations) {

		Serializer<?> serializer;
		for (KryoRegistration registration : resolvedRegistrations) {
			serializer = registration.getSerializer(kryo);

			if (serializer != null) {
				kryo.register(registration.getRegisteredClass(), serializer, kryo.getNextRegistrationId());
			} else {
				kryo.register(registration.getRegisteredClass(), kryo.getNextRegistrationId());
			}
		}
	}
}
