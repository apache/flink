/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.rabbitmq;

import org.apache.log4j.Level;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.faulttolerance.FaultToleranceType;
import eu.stratosphere.streaming.util.ClusterUtil;
import eu.stratosphere.streaming.util.LogUtils;

public class RMQTopology {

	public static class RMQSource extends UserSourceInvokable {
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

	public static class Sink extends UserSinkInvokable {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(StreamRecord record) throws Exception {
			System.out.println(record.getString(0));
		}
	}

	private static JobGraph getJobGraph() {

		JobGraphBuilder graphBuilder = new JobGraphBuilder("RMQ", FaultToleranceType.NONE);
		graphBuilder.setSource("Source", new RMQSource("localhost", "hello"), 1, 1);
		graphBuilder.setSink("Sink", Sink.class, 1, 1);

		graphBuilder.shuffleConnect("Source", "Sink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {

		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);
		ClusterUtil.runOnMiniCluster(getJobGraph());

	}
}