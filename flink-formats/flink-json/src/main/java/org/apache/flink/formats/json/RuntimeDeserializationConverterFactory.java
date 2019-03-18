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

package org.apache.flink.formats.json;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;

/**
 * Abstract factory that resolves {@link RowTypeInfo} into a converter that can be used
 * during runtime to convert {@link JsonNode}s into {@link org.apache.flink.types.Row}s.
 */
@PublicEvolving
interface RuntimeDeserializationConverterFactory {

	/**
	 * Runtime converter that maps between {@link JsonNode}s and Java objects.
	 */
	@FunctionalInterface
	interface DeserializationRuntimeConverter extends Serializable {
		Object convert(ObjectMapper mapper, JsonNode jsonNode);
	}

	/**
	 * Creates a converter based on the given {@link RowTypeInfo} that can be used during runtime to deserialize
	 * json into {@link org.apache.flink.types.Row}s.
	 */
	DeserializationRuntimeConverter getDeserializationRuntimeConverter(RowTypeInfo typeInfo, boolean failOnMissingField);
}
