package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.runtime.operators.Triggerable;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchemaWrapper;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class FlinkKafkaConsumer09WithPeriodicWM<T> extends AbstractFlinkKafkaConsumer09<T> implements Triggerable {

	protected AssignerWithPeriodicWatermarks<T> periodicWatermarkAssigner;

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param valueDeserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer09WithPeriodicWM(String topic, DeserializationSchema<T> valueDeserializer, Properties props,
											  AssignerWithPeriodicWatermarks<T> timestampAssigner) {
		this(Collections.singletonList(topic), valueDeserializer, props, timestampAssigner);
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
	 *
	 * This constructor allows passing a {@see KeyedDeserializationSchema} for reading key/value
	 * pairs, offsets, and topic names from Kafka.
	 *
	 * @param topic
	 *           The name of the topic that should be consumed.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties used to configure the Kafka consumer client, and the ZooKeeper client.
	 */
	public FlinkKafkaConsumer09WithPeriodicWM(String topic, KeyedDeserializationSchema<T> deserializer, Properties props,
											  AssignerWithPeriodicWatermarks<T> timestampAssigner) {
		super(Collections.singletonList(topic), deserializer, props);
		this.periodicWatermarkAssigner = timestampAssigner;
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
	 *
	 * This constructor allows passing multiple topics to the consumer.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer09WithPeriodicWM(List<String> topics, DeserializationSchema<T> deserializer, Properties props,
											  AssignerWithPeriodicWatermarks<T> timestampAssigner) {
		super(topics, new KeyedDeserializationSchemaWrapper<>(deserializer), props);
		this.periodicWatermarkAssigner = timestampAssigner;
	}

	/**
	 * Creates a new Kafka streaming source consumer for Kafka 0.9.x
	 *
	 * This constructor allows passing multiple topics and a key/value deserialization schema.
	 *
	 * @param topics
	 *           The Kafka topics to read from.
	 * @param deserializer
	 *           The keyed de-/serializer used to convert between Kafka's byte messages and Flink's objects.
	 * @param props
	 *           The properties that are used to configure both the fetcher and the offset handler.
	 */
	public FlinkKafkaConsumer09WithPeriodicWM(List<String> topics, KeyedDeserializationSchema<T> deserializer, Properties props,
											  AssignerWithPeriodicWatermarks<T> timestampAssigner) {
		super(topics, deserializer, props);
		this.periodicWatermarkAssigner = timestampAssigner;
	}

	@Override
	public void processElement(SourceContext<T> sourceContext, TopicPartition partitionInfo, T value) {

	}

	@Override
	public void trigger(long timestamp) throws Exception {

	}
}
