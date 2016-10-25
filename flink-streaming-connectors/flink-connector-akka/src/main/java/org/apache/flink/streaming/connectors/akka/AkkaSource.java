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

package org.apache.flink.streaming.connectors.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.StoppableFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.akka.utils.ReceiverActor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link SourceFunction} specialized to read messages
 * from Akka actors.
 */
public class AkkaSource extends RichSourceFunction<Object>
	implements StoppableFunction {

	private static final Logger LOG = LoggerFactory.getLogger(AkkaSource.class);

	private static final long serialVersionUID = 1L;

	// --- Fields set by the constructor

	private final Class<?> classForActor;

	private final String actorName;

	private final String urlOfPublisher;

	// --- Runtime fields
	private transient ActorSystem receiverActorSystem;
	private transient ActorRef receiverActor;
	private transient Object waitLock;
	private transient boolean running = true;

	protected transient boolean autoAck;

	/**
	 * Creates {@link AkkaSource} for Streaming
	 *
	 * @param actorName Receiver Actor name
	 * @param urlOfPublisher tcp url of the publisher or feeder actor
	 */
	public AkkaSource(String actorName,
					String urlOfPublisher) {
		super();
		this.classForActor = ReceiverActor.class;
		this.actorName = actorName;
		this.urlOfPublisher = urlOfPublisher;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		waitLock = new Object();
		receiverActorSystem = createDefaultActorSystem();

		RuntimeContext runtimeContext = getRuntimeContext();
		if (runtimeContext instanceof StreamingRuntimeContext
			&& ((StreamingRuntimeContext) runtimeContext).isCheckpointingEnabled()) {
			autoAck = false;
		} else {
			autoAck = true;
		}
	}

	@Override
	public void run(SourceContext<Object> ctx) throws Exception {
		LOG.info("Starting the Receiver actor {}", actorName);
		receiverActor = receiverActorSystem.actorOf(
			Props.create(classForActor, ctx, urlOfPublisher, autoAck), actorName);

		running = true;
		LOG.info("Started the Receiver actor {} successfully", actorName);

		while (running) {
			synchronized (waitLock) {
				waitLock.wait(100L);
			}
		}
	}

	@Override
	public void close() {
		this.running = false;
		LOG.info("Closing source");
		if (receiverActorSystem != null) {
			receiverActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
			receiverActorSystem.shutdown();
			receiverActorSystem.awaitTermination();
		}
		synchronized (waitLock) {
			waitLock.notify();
		}
	}

	@Override
	public void cancel() {
		LOG.info("Cancelling akka source");
		close();
	}

	@Override
	public void stop() {
		LOG.info("Stopping akka source");
		close();
	}

	/**
	 * Creates an actor system with default configurations for Receiver actor.
	 *
	 * @return Actor System instance with default configurations
	 */
	private ActorSystem createDefaultActorSystem() {
		String defaultActorSystemName = "receiver-actor-system";

		String configString = "akka.actor.provider = \"akka.remote.RemoteActorRefProvider\"\n" +
			"akka.remote.enabled-transports = [\"akka.remote.netty.tcp\"]";
		Config defaultConfig = ConfigFactory.parseString(configString);

		return ActorSystem.create(defaultActorSystemName, defaultConfig);
	}
}
