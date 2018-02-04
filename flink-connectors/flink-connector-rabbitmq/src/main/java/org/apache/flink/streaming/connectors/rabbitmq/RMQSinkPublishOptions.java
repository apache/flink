package org.apache.flink.streaming.connectors.rabbitmq;

import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * The message computation provides methods to compute the message routing key and/or the properties.
 *
 * @param <IN> The type of the data used by the sink.
 */
public interface RMQSinkPublishOptions<IN> {

	/**
	 * Compute the message's routing key from the data.
	 * @param a The data used by the sink
	 * @return The routing key of the message
	 */
	public String computeRoutingKey(IN a);

	/**
	 * Compute the message's properties from the data.
	 * @param a The data used by the sink
	 * @return The message's properties (can be null)
	 */
	public BasicProperties computeProperties(IN a);

	/**
	 * Compute the exchange from the data.
	 * @param a The data used by the sink
	 * @return The exchange to publish the message to
	 */
	public String computeExchange(IN a);

}
