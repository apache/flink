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

package org.apache.flink.formats.single.value;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.SingleValueValidator;
import org.apache.flink.table.factories.DeserializationSchemaFactory;
import org.apache.flink.table.factories.SerializationSchemaFactory;
import org.apache.flink.table.factories.TableFormatFactoryBase;
import org.apache.flink.types.Row;

import java.util.Map;

/**
 * SingleValueFormatFactory for single value.
 */
public class SingleValueFormatFactory extends TableFormatFactoryBase<Row>
		implements SerializationSchemaFactory<Row>, DeserializationSchemaFactory<Row> {

	public SingleValueFormatFactory() {
		super(SingleValueValidator.FORMAT_TYPE_VALUE, 1, true);
	}

	@Override
	public DeserializationSchema<Row> createDeserializationSchema(
		Map<String, String> properties) {
		TableSchema tableSchema = deriveSchema(properties);
		return new SingleValueRowSerializer(tableSchema);
	}

	@Override
	public SerializationSchema<Row> createSerializationSchema(
		Map<String, String> properties) {
		TableSchema tableSchema = deriveSchema(properties);
		return new SingleValueRowSerializer(tableSchema);
	}
}
