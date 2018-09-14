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

package org.apache.flink.streaming.util.serialization;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;

/**
 * Deserialization schema from JSON to {@link Row}.
 *
 * <p>Deserializes the <code>byte[]</code> messages as a JSON object and reads
 * the specified fields.
 *
 * <p>Failure during deserialization are forwarded as wrapped IOExceptions.
 *
 * @deprecated Please use {@link org.apache.flink.formats.json.JsonRowDeserializationSchema} in
 * the "flink-json" module.
 */
@PublicEvolving
@Deprecated
public class JsonRowDeserializationSchema extends org.apache.flink.formats.json.JsonRowDeserializationSchema {

	/**
	 * Creates a JSON deserialization schema for the given fields and types.
	 *
	 * @param typeInfo   Type information describing the result type. The field names are used
	 *                   to parse the JSON file and so are the types.
	 *
	 * @deprecated Please use {@link org.apache.flink.formats.json.JsonRowDeserializationSchema} in
	 * the "flink-json" module.
	 */
	@Deprecated
	public JsonRowDeserializationSchema(TypeInformation<Row> typeInfo) {
		super(typeInfo);
	}
}
