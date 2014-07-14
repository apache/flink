package eu.stratosphere.streaming.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class RMQSource extends UserSourceInvokable {
	private static final long serialVersionUID = 1L;

	private final String QUEUE_NAME;
	private final String HOST_NAME;

	StreamRecord record = new StreamRecord(new Tuple1<String>());

	public RMQSource(String HOST_NAME, String QUEUE_NAME) {
		this.HOST_NAME = HOST_NAME;
		this.QUEUE_NAME = QUEUE_NAME;

	}

	@Override
	public void invoke() throws Exception {

		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(HOST_NAME);
		Connection connection = factory.newConnection();
		Channel channel = connection.createChannel();

		channel.queueDeclare(QUEUE_NAME, false, false, false, null);

		QueueingConsumer consumer = new QueueingConsumer(channel);
		channel.basicConsume(QUEUE_NAME, true, consumer);

		while (true) {
			QueueingConsumer.Delivery delivery = consumer.nextDelivery();
			String message = new String(delivery.getBody());
			if (message.equals("quit")) {
				break;
			}
			record.setString(0, message);
			emit(record);
		}
		connection.close();
	}

}
