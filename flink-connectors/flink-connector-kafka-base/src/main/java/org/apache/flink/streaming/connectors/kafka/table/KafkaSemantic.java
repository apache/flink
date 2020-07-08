package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.annotation.Internal;

/**
 * Kafka sink semantic Enum.
 * */
@Internal
public enum KafkaSemantic {
	/**
	 * Semantic.EXACTLY_ONCE the Flink producer will write all messages in a Kafka transaction that will be
	 * committed to Kafka on a checkpoint.
	 */
	EXACTLY_ONCE,
	/**
	 * Semantic.AT_LEAST_ONCE the Flink producer will wait for all outstanding messages in the Kafka buffers
	 * to be acknowledged by the Kafka producer on a checkpoint.
	 */
	AT_LEAST_ONCE,
	/**
	 * Semantic.NONE means that nothing will be guaranteed. Messages can be lost and/or duplicated in case
	 * of failure.
	 */
	NONE;
}
