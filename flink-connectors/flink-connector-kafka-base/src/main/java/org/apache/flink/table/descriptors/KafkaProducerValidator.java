package org.apache.flink.table.descriptors;

public class KafkaProducerValidator extends KafkaValidator {

	/**
	 * For Kafka producer:
	 * Only need to check topic is specified or not.
	 */
	@Override
	public void validateTopicSetting(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_TOPIC, false, 1, Integer.MAX_VALUE);
	}
}
