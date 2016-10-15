/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.akka.utils;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FeederActor extends UntypedActor {

	public enum MessageTypes {
		SINGLE_DATA, ITERABLE_DATA, BYTES_DATA,
		SINGLE_DATA_WITH_TIMESTAMP
	}

	private static final Logger LOG = LoggerFactory.getLogger(FeederActor.class);

	private final MessageTypes messageType;

	public FeederActor(MessageTypes messageType) {
		this.messageType = messageType;
	}

	@Override
	public void onReceive(Object message) {
		if (message instanceof SubscribeReceiver) {
			ActorRef receiver = ((SubscribeReceiver) message).getReceiverActor();

			Object data;
			switch (messageType) {
				case SINGLE_DATA:
					data = createSingleDataMessage();
					break;
				case ITERABLE_DATA:
					data = createIterableOfMessages();
					break;
				case BYTES_DATA:
					data = createByteMessages();
					break;
				case SINGLE_DATA_WITH_TIMESTAMP:
					data = createTimestampMessage();
					break;
				default:
					throw new RuntimeException("Message format specified is incorrect");
			}
			receiver.tell(data, getSelf());
		} else if (message instanceof String) {
			Assert.assertEquals(message.toString(), "ack");
		} else if (message instanceof UnsubscribeReceiver) {
			LOG.info("Stop actor!");
		}
	}

	private Object createSingleDataMessage() {
		return Message.WELCOME_MESSAGE;
	}

	private List<Object> createIterableOfMessages() {
		List<Object> messages = new ArrayList<Object>();

		messages.add(Message.WELCOME_MESSAGE);
		messages.add(Message.FEEDER_MESSAGE);

		return messages;
	}

	private byte[] createByteMessages() {
		byte[] message = Message.WELCOME_MESSAGE.getBytes();
		return message;
	}

	private Tuple2<Object, Long> createTimestampMessage() {
		Tuple2<Object, Long> message = new Tuple2<Object, Long>();
		message.f0 = Message.WELCOME_MESSAGE;
		message.f1 = System.currentTimeMillis();

		return message;
	}
}
