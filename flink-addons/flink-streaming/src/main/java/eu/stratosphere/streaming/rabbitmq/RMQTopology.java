package eu.stratosphere.streaming.rabbitmq;

import java.net.InetSocketAddress;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class RMQTopology {

	public static class RMQSource extends UserSourceInvokable {
		StreamRecord record = new StreamRecord(new Tuple1<String>());

		@Override
		public void invoke() throws Exception {
			
			String QUEUE_NAME = "hello";
			ConnectionFactory factory = new ConnectionFactory();
		    factory.setHost("localhost");
		    Connection connection = factory.newConnection();
		    Channel channel = connection.createChannel();

		    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
		    
		    QueueingConsumer consumer = new QueueingConsumer(channel);
		    channel.basicConsume(QUEUE_NAME, true, consumer);

		    while (true) {
		      QueueingConsumer.Delivery delivery = consumer.nextDelivery();
		      String message = new String(delivery.getBody());
		      if(message.equals("quit")){
		    	  break;
		      }
		      record.setString(0, message);
		      emit(record);
		    }
		    connection.close();
		}

	}

	public static class Sink extends UserSinkInvokable {

		@Override
		public void invoke(StreamRecord record) throws Exception {
			System.out.println(record);
		}
	}

	private static JobGraph getJobGraph() throws Exception {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("RMQ");
		graphBuilder.setSource("Source", RMQSource.class, 1, 1);
		graphBuilder.setSink("Sink", Sink.class, 1, 1);

		graphBuilder.shuffleConnect("Source", "Sink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {

		try {
			JobGraph jG = getJobGraph();
			Configuration configuration = jG.getJobConfiguration();

			System.out.println("Running in Local mode");
			NepheleMiniCluster exec = new NepheleMiniCluster();

			exec.start();

			Client client = new Client(new InetSocketAddress("localhost", 6498), configuration);

			client.run(jG, true);

			exec.stop();
		} catch (Exception e) {

		}
	}

}
