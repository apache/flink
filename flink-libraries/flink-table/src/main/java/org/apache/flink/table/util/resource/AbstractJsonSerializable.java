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

package org.apache.flink.table.util.resource;

import org.apache.flink.util.StringUtils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * Base class for object that can be serialized to json.
 */
public class AbstractJsonSerializable {

	public static <T> T fromJson(String json, Class c) {
		return fromJson(json, c, false);
	}

	public static <T> T fromJson(String json, Class c, boolean ignoreUnknownProperties) {
		if (StringUtils.isNullOrWhitespaceOnly(json)) {
			return null;
		}

		try {
			ObjectMapper objectMapper = new ObjectMapper();
			objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
			if (ignoreUnknownProperties) {
				objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
			} else {
				objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true);
			}
			return (T) objectMapper.readValue(json, c);
		} catch (IOException e) {
			// DON'T add the json string to error message to spam logging
			// e will contain the json string
			throw new IllegalArgumentException("error converting from JSON string.", e);
		}
	}

	public static String toJson(Object value) {
		try {
			return new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(value);
		} catch (JsonProcessingException e) {
			throw new AssertionError("Writing a valid object as JSON string should not fail: " + e.getMessage());
		}
	}

	public <T> T copy() {
		return copy(getClass());
	}

	public <T> T copy(Class cl) {
		try {
			return fromJson(toString(), cl, !getClass().equals(cl));
		} catch (IllegalArgumentException ex) {
			Throwable e = ex.getCause();
			assert e instanceof IOException;
			if (e instanceof JsonMappingException
					&& e.getMessage().contains("can not instantiate") && cl.equals(getClass())) {
				throw new IllegalArgumentException(
						String.format(
								"Error copying to type '%s'.  Explicitly passing a target class with " +
										"default constructor might help.",
								cl.getTypeName()),
						e);
			} else {
				throw new AssertionError("reading a valid JSON string should not fail: " + e.getMessage());
			}
		}
	}

	// correct but inefficient implementation; override as needed
	@Override
	public int hashCode() {
		return toString().hashCode();
	}

	// correct but inefficient implementation; override as needed
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null || getClass() != obj.getClass()) {
			return false;
		}
		return toString().equals(obj.toString());
	}

	@Override
	public String toString() {
		return toJson(this);
	}

}

