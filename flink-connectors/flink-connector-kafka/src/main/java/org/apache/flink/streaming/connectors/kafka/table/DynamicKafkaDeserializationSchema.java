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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;

/**
 * A specific {@link KafkaSerializationSchema} for {@link KafkaDynamicSource}.
 */
class DynamicKafkaDeserializationSchema implements KafkaDeserializationSchema<RowData> {

	private static final long serialVersionUID = 1L;

	private final DeserializationSchema<RowData> valueDeserialization;

	private final boolean hasMetadata;

	private final MetadataAppendingCollector metadataAppendingCollector;

	private final TypeInformation<RowData> producedTypeInfo;

	DynamicKafkaDeserializationSchema(
			DeserializationSchema<RowData> valueDeserialization,
			boolean hasMetadata,
			MetadataConverter[] metadataConverters,
			TypeInformation<RowData> producedTypeInfo) {
		this.hasMetadata = hasMetadata;
		this.valueDeserialization = valueDeserialization;
		this.metadataAppendingCollector = new MetadataAppendingCollector(metadataConverters);
		this.producedTypeInfo = producedTypeInfo;
	}

	@Override
	public void open(DeserializationSchema.InitializationContext context) throws Exception {
		valueDeserialization.open(context);
	}

	@Override
	public boolean isEndOfStream(RowData nextElement) {
		return false;
	}

	@Override
	public RowData deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
		throw new IllegalStateException("A collector is required for deserializing.");
	}

	@Override
	public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<RowData> collector) throws Exception {
		// shortcut if no metadata is required
		if (!hasMetadata) {
			valueDeserialization.deserialize(record.value(), collector);
		} else {
			metadataAppendingCollector.inputRecord = record;
			metadataAppendingCollector.outputCollector = collector;
			valueDeserialization.deserialize(record.value(), metadataAppendingCollector);
		}
	}

	@Override
	public TypeInformation<RowData> getProducedType() {
		return producedTypeInfo;
	}

	// --------------------------------------------------------------------------------------------

	interface MetadataConverter extends Serializable {
		Object read(ConsumerRecord<?, ?> record);
	}

	// --------------------------------------------------------------------------------------------

	private static final class MetadataAppendingCollector implements Collector<RowData>, Serializable {

		private static final long serialVersionUID = 1L;

		private final MetadataConverter[] metadataConverters;

		private transient ConsumerRecord<?, ?> inputRecord;

		private transient Collector<RowData> outputCollector;

		MetadataAppendingCollector(MetadataConverter[] metadataConverters) {
			this.metadataConverters = metadataConverters;
		}

		@Override
		public void collect(RowData physicalRow) {
			final GenericRowData genericPhysicalRow = (GenericRowData) physicalRow;
			final int physicalArity = physicalRow.getArity();
			final int metadataArity = metadataConverters.length;

			final GenericRowData producedRow = new GenericRowData(
					physicalRow.getRowKind(),
					physicalArity + metadataArity);

			for (int i = 0; i < physicalArity; i++) {
				producedRow.setField(i, genericPhysicalRow.getField(i));
			}

			for (int i = 0; i < metadataArity; i++) {
				producedRow.setField(i + physicalArity, metadataConverters[i].read(inputRecord));
			}

			outputCollector.collect(producedRow);
		}

		@Override
		public void close() {
			// nothing to do
		}
	}
}
