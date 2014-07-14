package eu.stratosphere.streaming.rabbitmq;

import java.io.IOException;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ConsumerCancelledException;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

/**
 * Source for reading messages from a RabbitMQ queue. The source currently only support string messages. Other types will be added soon.
 *
 */
public class RMQSource extends UserSourceInvokable {
	private static final long serialVersionUID = 1L;

	private final String QUEUE_NAME;
	private final String HOST_NAME;

	private transient ConnectionFactory factory;
	private transient Connection connection;
	private transient Channel channel;
	private transient QueueingConsumer consumer;
	private transient QueueingConsumer.Delivery delivery;

	private transient String message;

	StreamRecord record = new StreamRecord(new Tuple1<String>());

	public RMQSource(String HOST_NAME, String QUEUE_NAME) {
		this.HOST_NAME = HOST_NAME;
		this.QUEUE_NAME = QUEUE_NAME;
	}

	private void initializeConnection() {
		factory = new ConnectionFactory();
		factory.setHost(HOST_NAME);
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			consumer = new QueueingConsumer(channel);
			channel.basicConsume(QUEUE_NAME, true, consumer);
		} catch (IOException e) {
		}
	}

	@Override
	public void invoke() {

		initializeConnection();

		while (true) {

			try {
				delivery = consumer.nextDelivery();
			} catch (ShutdownSignalException e) {
				e.printStackTrace();
				break;
			} catch (ConsumerCancelledException e) {
				e.printStackTrace();
				break;
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			message = new String(delivery.getBody());

			if (message.equals("quit")) {
				break;
			}

			record.setString(0, message);
			emit(record);
		}

		try {
			connection.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
