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

import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.akka.utils.FeederActor;
import org.apache.flink.streaming.connectors.akka.utils.Message;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
public class AkkaSourceTest {
	private AkkaSource source;

	private static final String feederActorName = "JavaFeederActor";
	private static final String receiverActorName = "receiverActor";
	private static final String urlOfFeeder =
		"akka.tcp://feederActorSystem@127.0.0.1:5150/user/" + feederActorName;
	private ActorSystem feederActorSystem;

	private Configuration config = new Configuration();

	private Thread sourceThread;

	private SourceFunction.SourceContext<Object> sourceContext;

	private volatile Exception exception;

	@Before
	public void beforeTest() throws Exception {
		feederActorSystem = ActorSystem.create("feederActorSystem",
			getFeederActorConfig());

		source = new AkkaTestSource();
		source.open(config);

		sourceContext = new DummySourceContext();

		sourceThread = new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					SourceFunction.SourceContext<Object> sourceContext =
						new DummySourceContext();
					source.run(sourceContext);
				} catch (Exception e) {
					exception = e;
				}
			}
		});
	}

	@After
	public void afterTest() throws Exception {
		feederActorSystem.shutdown();
		feederActorSystem.awaitTermination();

		source.cancel();
		sourceThread.join();
	}

	@Test
	public void testWithSingleData() throws Exception {
		feederActorSystem.actorOf(
			Props.create(FeederActor.class, FeederActor.MessageTypes.SINGLE_DATA),
			feederActorName);

		source.autoAck = false;
		sourceThread.start();

		while (DummySourceContext.numElementsCollected != 1) {
			Thread.sleep(5);
		}
		List<Object> message = DummySourceContext.message;
		Assert.assertEquals(message.get(0).toString(), Message.WELCOME_MESSAGE);
	}

	@Test
	public void testWithIterableData() throws Exception {
		feederActorSystem.actorOf(
			Props.create(FeederActor.class, FeederActor.MessageTypes.ITERABLE_DATA),
			feederActorName);

		source.autoAck = false;
		sourceThread.start();

		while (DummySourceContext.numElementsCollected != 2) {
			Thread.sleep(5);
		}

		List<Object> messages = DummySourceContext.message;
		Assert.assertEquals(messages.get(0).toString(), Message.WELCOME_MESSAGE);
		Assert.assertEquals(messages.get(1).toString(), Message.FEEDER_MESSAGE);
	}

	@Test
	public void testWithByteArrayData() throws Exception {
		feederActorSystem.actorOf(
			Props.create(FeederActor.class, FeederActor.MessageTypes.BYTES_DATA),
			feederActorName);

		source.autoAck = false;
		sourceThread.start();

		while (DummySourceContext.numElementsCollected != 1) {
			Thread.sleep(5);
		}

		List<Object> message = DummySourceContext.message;
		if (message.get(0) instanceof byte[]) {
			byte[] data = (byte[]) message.get(0);
			Assert.assertEquals(new String(data), Message.WELCOME_MESSAGE);
		}
	}

	@Test
	public void testWithSingleDataWithTimestamp() throws Exception {
		feederActorSystem.actorOf(
			Props.create(FeederActor.class, FeederActor.MessageTypes.SINGLE_DATA_WITH_TIMESTAMP),
			feederActorName);

		source.autoAck = false;
		sourceThread.start();

		while (DummySourceContext.numElementsCollected != 1) {
			Thread.sleep(5);
		}

		List<Object> message = DummySourceContext.message;
		Assert.assertEquals(message.get(0).toString(), Message.WELCOME_MESSAGE);
	}

	@Test
	public void testAcksWithSingleData() throws Exception {
		feederActorSystem.actorOf(
			Props.create(FeederActor.class, FeederActor.MessageTypes.SINGLE_DATA),
			feederActorName);

		source.autoAck = true;
		sourceThread.start();

		while (DummySourceContext.numElementsCollected != 1) {
			Thread.sleep(5);
		}
		// assertion tested in FeederActor
	}

	private class AkkaTestSource extends AkkaSource {

		public AkkaTestSource() {
			super(receiverActorName, urlOfFeeder);
		}

		@Override
		public RuntimeContext getRuntimeContext() {
			return Mockito.mock(StreamingRuntimeContext.class);
		}
	}

	private static class DummySourceContext implements SourceFunction.SourceContext<Object> {
		private static final Object lock = new Object();

		private static long numElementsCollected;

		private static List<Object> message;

		public DummySourceContext() {
			numElementsCollected = 0;
			message = new ArrayList<Object>();
		}

		@Override
		public void collect(Object element) {
			message.add(element);
			numElementsCollected++;
		}

		@Override
		public void collectWithTimestamp(Object element, long timestamp) {
			message.add(element);
			numElementsCollected++;
		}

		@Override
		public void emitWatermark(Watermark mark) {

		}

		@Override
		public Object getCheckpointLock() {
			return lock;
		}

		@Override
		public void close() {

		}
	}

	private Config getFeederActorConfig() {
		String configFile = getClass().getClassLoader()
			.getResource("feeder_actor.conf").getFile();
		Config config = ConfigFactory.parseFile(new File(configFile));
		return config;
	}
}
