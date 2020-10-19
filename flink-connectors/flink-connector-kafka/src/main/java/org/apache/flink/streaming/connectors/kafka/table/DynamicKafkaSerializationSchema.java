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

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * A specific {@link KafkaSerializationSchema} for {@link KafkaDynamicSink}.
 */
class DynamicKafkaSerializationSchema
		implements KafkaSerializationSchema<RowData>, KafkaContextAware<RowData> {

	private static final long serialVersionUID = 1L;

	private final @Nullable FlinkKafkaPartitioner<RowData> partitioner;

	private final String topic;

	private final SerializationSchema<RowData> valueSerialization;

	private final boolean hasMetadata;

	/**
	 * Contains the position for each value of {@link KafkaDynamicSink.WritableMetadata} in the consumed row or
	 * -1 if this metadata key is not used.
	 */
	private final int[] metadataPositions;

	private final RowData.FieldGetter[] physicalFieldGetters;

	private int[] partitions;

	private int parallelInstanceId;

	private int numParallelInstances;

	DynamicKafkaSerializationSchema(
			String topic,
			@Nullable FlinkKafkaPartitioner<RowData> partitioner,
			SerializationSchema<RowData> valueSerialization,
			boolean hasMetadata,
			int[] metadataPositions,
			RowData.FieldGetter[] physicalFieldGetters) {
		this.topic = topic;
		this.partitioner = partitioner;
		this.valueSerialization = valueSerialization;
		this.hasMetadata = hasMetadata;
		this.metadataPositions = metadataPositions;
		this.physicalFieldGetters = physicalFieldGetters;
	}

	@Override
	public void open(SerializationSchema.InitializationContext context) throws Exception {
		valueSerialization.open(context);
		if (partitioner != null) {
			partitioner.open(parallelInstanceId, numParallelInstances);
		}
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(RowData consumedRow, @Nullable Long timestamp) {
		final RowData physicalRow;
		// shortcut if no metadata is required
		if (!hasMetadata) {
			physicalRow = consumedRow;
		} else {
			final int physicalArity = physicalFieldGetters.length;
			final GenericRowData genericRowData = new GenericRowData(
					consumedRow.getRowKind(),
					physicalArity);
			for (int i = 0; i < physicalArity; i++) {
				genericRowData.setField(i, physicalFieldGetters[i].getFieldOrNull(consumedRow));
			}
			physicalRow = genericRowData;
		}

		final byte[] valueSerialized = valueSerialization.serialize(physicalRow);

		final Integer partition;
		if (partitioner != null) {
			partition = partitioner.partition(physicalRow, null, valueSerialized, topic, partitions);
		} else {
			partition = null;
		}

		// shortcut if no metadata is required
		if (!hasMetadata) {
			return new ProducerRecord<>(topic, partition, null, null, valueSerialized);
		}
		return new ProducerRecord<>(
				topic,
				partition,
				readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.TIMESTAMP),
				null,
				valueSerialized,
				readMetadata(consumedRow, KafkaDynamicSink.WritableMetadata.HEADERS));
	}

	@Override
	public void setParallelInstanceId(int parallelInstanceId) {
		this.parallelInstanceId = parallelInstanceId;
	}

	@Override
	public void setNumParallelInstances(int numParallelInstances) {
		this.numParallelInstances = numParallelInstances;
	}

	@Override
	public void setPartitions(int[] partitions) {
		this.partitions = partitions;
	}

	@Override
	public String getTargetTopic(RowData element) {
		return topic;
	}

	@SuppressWarnings("unchecked")
	private <T> T readMetadata(RowData consumedRow, KafkaDynamicSink.WritableMetadata metadata) {
		final int pos = metadataPositions[metadata.ordinal()];
		if (pos < 0) {
			return null;
		}
		return (T) metadata.converter.read(consumedRow, pos);
	}

	// --------------------------------------------------------------------------------------------

	interface MetadataConverter extends Serializable {
		Object read(RowData consumedRow, int pos);
	}
}
