/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
 * Serialization schema that serializes an object into a JSON bytes.
 *
 * <p>Serializes the input {@link Row} object into a JSON string and
 * converts it into <code>byte[]</code>.
 *
 * <p>Result <code>byte[]</code> messages can be deserialized using
 * {@link JsonRowDeserializationSchema}.
 *
 * @deprecated Please use {@link org.apache.flink.formats.json.JsonRowSerializationSchema} in
 * the "flink-json" module.
 */
@PublicEvolving
@Deprecated
public class JsonRowSerializationSchema extends org.apache.flink.formats.json.JsonRowSerializationSchema {

	/**
	 * Creates a JSON serialization schema for the given fields and types.
	 *
	 * @param typeInfo The schema of the rows to encode.
	 *
	 * @deprecated Please use {@link org.apache.flink.formats.json.JsonRowSerializationSchema} in
	 * the "flink-json" module.
	 */
	@Deprecated
	public JsonRowSerializationSchema(TypeInformation<Row> typeInfo) {
		super(typeInfo);
	}
}
