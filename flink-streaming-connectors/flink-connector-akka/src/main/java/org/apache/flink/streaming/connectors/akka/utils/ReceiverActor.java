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
import akka.actor.ActorSelection;
import akka.actor.UntypedActor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;

import java.util.Iterator;

/**
 * Generalized receiver actor which receives messages
 * from the feeder or publisher actor.
 */
public class ReceiverActor extends UntypedActor {
	// --- Fields set by the constructor
	private final SourceContext<Object> ctx;

	private final String urlOfPublisher;

	private final boolean autoAck;

	// --- Runtime fields
	private ActorSelection remotePublisher;

	public ReceiverActor(SourceContext<Object> ctx,
						String urlOfPublisher,
						boolean autoAck) {
		this.ctx = ctx;
		this.urlOfPublisher = urlOfPublisher;
		this.autoAck = autoAck;
	}

	@Override
	public void preStart() throws Exception {
		remotePublisher = getContext().actorSelection(urlOfPublisher);
		remotePublisher.tell(new SubscribeReceiver(getSelf()), getSelf());
	}

	@SuppressWarnings("unchecked")
	@Override
	public void onReceive(Object message)
		throws Exception {
		if (message instanceof Iterable) {
			collect((Iterable<Object>) message);
		} else if (message instanceof byte[]) {
			byte[] messageBytes = (byte[]) message;
			collect(messageBytes);
		} else if (message instanceof Tuple2) {
			Tuple2<Object, Long> messageTuple = (Tuple2<Object, Long>) message;
			collect(messageTuple.f0, messageTuple.f1);
		}	else {
			collect(message);
		}

		if (autoAck) {
			getSender().tell("ack", getSelf());
		}
	}

	/**
	 * To handle {@link Iterable} data
	 *
	 * @param data data received from feeder actor
	 */
	private void collect(Iterable<Object> data) {
		Iterator<Object> iterator = data.iterator();
		while (iterator.hasNext()) {
			ctx.collect(iterator.next());
		}
	}

	/**
	 * To handle byte array data
	 *
	 * @param bytes data received from feeder actor
	 */
	private void collect(byte[] bytes) {
		ctx.collect(bytes);
	}

	/**
	 * To handle single data
	 * @param data data received from feeder actor
	 */
	private void collect(Object data) {
		ctx.collect(data);
	}

	/**
	 * To handle data with timestamp
	 *
	 * @param data data received from feeder actor
	 * @param timestamp timestamp received from feeder actor
	 */
	private void collect(Object data, long timestamp) {
		ctx.collectWithTimestamp(data, timestamp);
	}

	@Override
	public void postStop() throws Exception {
		remotePublisher.tell(new UnsubscribeReceiver(ActorRef.noSender()),
			ActorRef.noSender());
	}
}
