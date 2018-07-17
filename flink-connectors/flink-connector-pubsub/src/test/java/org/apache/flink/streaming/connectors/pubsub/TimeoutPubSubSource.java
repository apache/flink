package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.api.common.serialization.DeserializationSchema;

/**
 *
 * @param <OUT>
 */
public class TimeoutPubSubSource<OUT> extends PubSubSource<OUT> {
	TimeoutPubSubSource(SubscriberWrapper subscriberWrapper, DeserializationSchema<OUT> deserializationSchema, PubSubSourceBuilder.Mode mode) {
		super(subscriberWrapper, deserializationSchema, mode);
	}
}
