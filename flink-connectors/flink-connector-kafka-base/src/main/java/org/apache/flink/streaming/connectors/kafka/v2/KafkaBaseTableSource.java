/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.connectors.kafka.v2;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.v2.common.SourceFunctionTableSource;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;

import java.util.List;
import java.util.Properties;

/** Kafka base table source. */
public abstract class KafkaBaseTableSource extends SourceFunctionTableSource<GenericRow> {
	protected List<String> topic;
	protected String topicPattern;
	protected Properties properties;
	protected StartupMode startupMode = StartupMode.GROUP_OFFSETS;
	protected long startTimeStamp = -1;
	protected boolean isFinite = false;
	protected BaseRowTypeInfo baseRowTypeInfo;

	public KafkaBaseTableSource(
			List<String> topic,
			String topicPattern,
			Properties properties,
			StartupMode startupMode,
			long startTimeStamp,
			boolean isFinite,
			BaseRowTypeInfo baseRowTypeInfo) {
		this.topic = topic;
		this.topicPattern = topicPattern;
		this.properties = properties;
		this.startupMode = startupMode;
		this.startTimeStamp = startTimeStamp;
		this.isFinite = isFinite;
		this.baseRowTypeInfo = baseRowTypeInfo;
	}

	public DataStream<GenericRow> getBoundedStream(StreamExecutionEnvironment execEnv) {
		DataStreamSource<GenericRow> boundedStreamSource = execEnv.addSource(getSourceFunction(),
				String.format("%s-%s", explainSource(), BATCH_TAG),
				getProducedType());
		int parallelism = getTopicPartitionSize();
		boundedStreamSource.setParallelism(parallelism);
		boundedStreamSource.getTransformation().setMaxParallelism(parallelism);
		return boundedStreamSource;

	}

	@Override
	public SourceFunction getSourceFunction() {
		FlinkKafkaConsumerBase consumer = createKafkaConsumer();
		switch (startupMode) {
			case LATEST:
				consumer.setStartFromLatest();
				break;
			case EARLIEST:
				consumer.setStartFromEarliest();
				break;
			case GROUP_OFFSETS:
				consumer.setStartFromGroupOffsets();
				break;
			case SPECIFIC_OFFSETS:
				break;
		}
		consumer.setCommitOffsetsOnCheckpoints(true);
		// consumer.setStopAtLatest(isFinite);
		return consumer;
	}

	public abstract FlinkKafkaConsumerBase createKafkaConsumer();

	public abstract int getTopicPartitionSize();
}
