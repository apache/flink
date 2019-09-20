/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.PropertyAccessor;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.lang.reflect.Type;

/**
 * Wrapper class of a JSON library to provide the unified json format.
 *
 * <p>Currently, we use jackson as the json library.
 */
public class JsonConverter {
	/**
	 * An instance of ObjectMapper in jackson.
	 */
	private static final ObjectMapper JSON_INSTANCE = new ObjectMapper()
		.setVisibility(PropertyAccessor.ALL, Visibility.NONE)
		.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);

	/**
	 * serialize an object to json.
	 *
	 * @param src the object serialized as json
	 * @return the json string
	 */
	public static String toJson(Object src) {
		try {
			return JSON_INSTANCE.writeValueAsString(src);
		} catch (JsonProcessingException e) {
			throw new IllegalJsonFormatException("Serialize object to json fail.", e);
		}
	}

	/**
	 * Deserialize an object from json using the {@link Class} of object.
	 *
	 * @param json     the json string
	 * @param classOfT the class of object
	 * @param <T>      the type of object
	 * @return the deserialized object
	 */
	public static <T> T fromJson(String json, Class<T> classOfT) {
		return fromJson(json, (Type) classOfT);
	}

	/**
	 * Deserialize an object from json using the {@link Type} of object.
	 *
	 * @param json    the json string
	 * @param typeOfT the type class of object
	 * @param <T>     the type of object
	 * @return the deserialized object
	 */
	public static <T> T fromJson(String json, Type typeOfT) {
		try {
			return JSON_INSTANCE.readValue(json, JSON_INSTANCE.getTypeFactory().constructType(typeOfT));
		} catch (IOException e) {
			throw new IllegalJsonFormatException("Deserialize json to object fail. json: " + json, e);
		}
	}

	/**
	 * Exception to indict the json format error.
	 */
	public static class IllegalJsonFormatException extends IllegalArgumentException {
		IllegalJsonFormatException(String message, Throwable cause) {
			super(message, cause);
		}
	}
}
