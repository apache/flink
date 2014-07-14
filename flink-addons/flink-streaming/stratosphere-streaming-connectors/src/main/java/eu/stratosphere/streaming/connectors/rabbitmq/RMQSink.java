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

package eu.stratosphere.streaming.connectors.rabbitmq;

import java.io.IOException;

import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.streaming.api.function.SinkFunction;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

/**
 * Source for sending messages to a RabbitMQ queue. The source currently only
 * support string messages. Other types will be added soon.
 * 
 */
public class RMQSink extends SinkFunction<Tuple1<String>>{
	private static final long serialVersionUID = 1L;
	
	private String QUEUE_NAME;
	private String HOST_NAME;
	private transient ConnectionFactory factory;
	private transient Connection connection;
	private transient Channel channel;
	
	
	public RMQSink(String HOST_NAME, String QUEUE_NAME) {
		this.HOST_NAME = HOST_NAME;
		this.QUEUE_NAME = QUEUE_NAME;
	}
	
	public void initializeConnection(){
		factory = new ConnectionFactory();
		factory.setHost(HOST_NAME);
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			
			
		    
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	@Override
	public void invoke(Tuple1<String> tuple) {
		
		initializeConnection();
		
		try {
			channel.queueDeclare(QUEUE_NAME, false, false, false, null);
			String message = tuple.f0;
		    channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	    
		
		try {
			channel.close();
			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	    
		
	}

}
