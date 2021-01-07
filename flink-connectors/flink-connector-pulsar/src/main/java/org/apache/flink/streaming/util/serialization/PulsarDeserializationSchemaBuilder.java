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
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.connectors.pulsar.util.TypeInformationUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;

import org.apache.pulsar.client.api.Schema;

import java.io.Serializable;

/**
 * Pulsar deserialization schema builder.
 *
 * @param <V>
 */
@PublicEvolving
public class PulsarDeserializationSchemaBuilder<V> implements Serializable {

	private DeserializationSchema<V> valueDeserializer;

	private DataType dataType;

	private Class<V> recordClass;

	private Schema<V> pulsarSchema;

	public PulsarDeserializationSchemaBuilder() {
	}

	public PulsarDeserializationSchemaBuilder<V> setValueDeserializer(DeserializationSchema<V> valueDeserializer) {
		this.valueDeserializer = valueDeserializer;
		return this;
	}

	public PulsarDeserializationSchemaBuilder<V> setDataType(DataType dataType) {
		this.dataType = dataType;
		return this;
	}

	public PulsarDeserializationSchemaBuilder<V> setRecordClass(Class<V> recordClass) {
		this.recordClass = recordClass;
		return this;
	}

	public PulsarDeserializationSchema<V> build() {
		if (dataType == null) {
			dataType = DataTypes
				.RAW(TypeInformationUtils.getTypesAsRow(recordClass))
				.bridgedTo(recordClass);
		}
		return new PulsarDeserializationSchemaWrapper<>(valueDeserializer, dataType);
	}
}
