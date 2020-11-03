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
import org.apache.flink.types.DeserializationException;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * A specific {@link KafkaSerializationSchema} for {@link KafkaDynamicSource}.
 */
class DynamicKafkaDeserializationSchema implements KafkaDeserializationSchema<RowData> {

	private static final long serialVersionUID = 1L;

	private final @Nullable DeserializationSchema<RowData> keyDeserialization;

	private final DeserializationSchema<RowData> valueDeserialization;

	private final boolean hasMetadata;

	private final BufferingCollector keyCollector;

	private final OutputProjectionCollector outputCollector;

	private final TypeInformation<RowData> producedTypeInfo;

	private final boolean upsertMode;

	DynamicKafkaDeserializationSchema(
			int physicalArity,
			@Nullable DeserializationSchema<RowData> keyDeserialization,
			int[] keyProjection,
			DeserializationSchema<RowData> valueDeserialization,
			int[] valueProjection,
			boolean hasMetadata,
			MetadataConverter[] metadataConverters,
			TypeInformation<RowData> producedTypeInfo,
			boolean upsertMode) {
		if (upsertMode) {
			Preconditions.checkArgument(
					keyDeserialization != null && keyProjection.length > 0,
					"Key must be set in upsert mode for deserialization schema.");
		}
		this.keyDeserialization = keyDeserialization;
		this.valueDeserialization = valueDeserialization;
		this.hasMetadata = hasMetadata;
		this.keyCollector = new BufferingCollector();
		this.outputCollector = new OutputProjectionCollector(
				physicalArity,
				keyProjection,
				valueProjection,
				metadataConverters,
				upsertMode);
		this.producedTypeInfo = producedTypeInfo;
		this.upsertMode = upsertMode;
	}

	@Override
	public void open(DeserializationSchema.InitializationContext context) throws Exception {
		if (keyDeserialization != null) {
			keyDeserialization.open(context);
		}
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
		// shortcut in case no output projection is required,
		// also not for a cartesian product with the keys
		if (keyDeserialization == null && !hasMetadata) {
			valueDeserialization.deserialize(record.value(), collector);
			return;
		}

		// buffer key(s)
		if (keyDeserialization != null) {
			keyDeserialization.deserialize(record.key(), keyCollector);
		}

		// project output while emitting values
		outputCollector.inputRecord = record;
		outputCollector.physicalKeyRows = keyCollector.buffer;
		outputCollector.outputCollector = collector;
		if (record.value() == null && upsertMode) {
			// collect tombstone messages in upsert mode by hand
			outputCollector.collect(null);
		} else {
			valueDeserialization.deserialize(record.value(), outputCollector);
		}
		keyCollector.buffer.clear();
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

	private static final class BufferingCollector implements Collector<RowData>, Serializable {

		private static final long serialVersionUID = 1L;

		private final List<RowData> buffer = new ArrayList<>();

		@Override
		public void collect(RowData record) {
			buffer.add(record);
		}

		@Override
		public void close() {
			// nothing to do
		}
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Emits a row with key, value, and metadata fields.
	 *
	 * <p>The collector is able to handle the following kinds of keys:
	 * <ul>
	 *     <li>No key is used.
	 *     <li>A key is used.
	 *     <li>The deserialization schema emits multiple keys.
	 *     <li>Keys and values have overlapping fields.
	 *     <li>Keys are used and value is null.
	 * </ul>
	 */
	private static final class OutputProjectionCollector implements Collector<RowData>, Serializable {

		private static final long serialVersionUID = 1L;

		private final int physicalArity;

		private final int[] keyProjection;

		private final int[] valueProjection;

		private final MetadataConverter[] metadataConverters;

		private final boolean upsertMode;

		private transient ConsumerRecord<?, ?> inputRecord;

		private transient List<RowData> physicalKeyRows;

		private transient Collector<RowData> outputCollector;

		OutputProjectionCollector(
				int physicalArity,
				int[] keyProjection,
				int[] valueProjection,
				MetadataConverter[] metadataConverters,
				boolean upsertMode) {
			this.physicalArity = physicalArity;
			this.keyProjection = keyProjection;
			this.valueProjection = valueProjection;
			this.metadataConverters = metadataConverters;
			this.upsertMode = upsertMode;
		}

		@Override
		public void collect(RowData physicalValueRow) {
			// no key defined
			if (keyProjection.length == 0) {
				emitRow(null, (GenericRowData) physicalValueRow);
				return;
			}

			// otherwise emit a value for each key
			for (RowData physicalKeyRow : physicalKeyRows) {
				emitRow((GenericRowData) physicalKeyRow, (GenericRowData) physicalValueRow);
			}
		}

		@Override
		public void close() {
			// nothing to do
		}

		private void emitRow(@Nullable GenericRowData physicalKeyRow, @Nullable GenericRowData physicalValueRow) {
			final RowKind rowKind;
			if (physicalValueRow == null) {
				if (upsertMode) {
					rowKind = RowKind.DELETE;
				} else {
					throw new DeserializationException(
							"Invalid null value received in non-upsert mode. Could not to set row kind for output record.");
				}
			} else {
				rowKind = physicalValueRow.getRowKind();
			}

			final int metadataArity = metadataConverters.length;
			final GenericRowData producedRow = new GenericRowData(
					rowKind,
					physicalArity + metadataArity);

			for (int keyPos = 0; keyPos < keyProjection.length; keyPos++) {
				assert physicalKeyRow != null;
				producedRow.setField(keyProjection[keyPos], physicalKeyRow.getField(keyPos));
			}

			if (physicalValueRow != null) {
				for (int valuePos = 0; valuePos < valueProjection.length; valuePos++) {
					producedRow.setField(valueProjection[valuePos], physicalValueRow.getField(valuePos));
				}
			}

			for (int metadataPos = 0; metadataPos < metadataArity; metadataPos++) {
				producedRow.setField(physicalArity + metadataPos, metadataConverters[metadataPos].read(inputRecord));
			}

			outputCollector.collect(producedRow);
		}
	}
}
