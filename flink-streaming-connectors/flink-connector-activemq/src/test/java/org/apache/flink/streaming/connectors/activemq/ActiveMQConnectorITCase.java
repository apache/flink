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

package org.apache.flink.streaming.connectors.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.AvroSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.Serializers;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;
import org.apache.flink.test.util.SuccessException;

import static org.apache.flink.test.util.TestUtils.tryExecute;


public class ActiveMQConnectorITCase {

	public static final int MESSAGES_NUM = 100;
	public static final String QUEUE_NAME = "queue";
	private static ForkableFlinkMiniCluster flink;
	private static int flinkPort;

	@BeforeClass
	public static void beforeClass() {
		// start also a re-usable Flink mini cluster
		Configuration flinkConfig = new Configuration();
		flinkConfig.setInteger(ConfigConstants.LOCAL_NUMBER_TASK_MANAGER, 1);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, 8);
		flinkConfig.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 16);
		flinkConfig.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s");
		flinkConfig.setString(ConfigConstants.METRICS_REPORTER_CLASS, "org.apache.flink.metrics.reporter.JMXReporter");

		flink = new ForkableFlinkMiniCluster(flinkConfig, false);
		flink.start();

		flinkPort = flink.getLeaderRPCPort();
	}

	@AfterClass
	public static void afterClass() {
		flinkPort = -1;
		if (flink != null) {
			flink.shutdown();
		}
	}

	@Test
	public void testAMQTopology() throws Exception {
		StreamExecutionEnvironment env = createExecutionEnvironment();
		createProducerTopology(env);
		createConsumerTopology(env);
		tryExecute(env, "AMQTest");
	}

	private StreamExecutionEnvironment createExecutionEnvironment() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkPort);
		env.setRestartStrategy(RestartStrategies.noRestart());
		env.getConfig().disableSysoutLogging();
		return env;
	}

	private void createProducerTopology(StreamExecutionEnvironment env) {
		DataStreamSource<String> stream = env.addSource(new SourceFunction<String>() {
			@Override
			public void run(SourceContext<String> ctx) throws Exception {
				for (int i = 0; i < MESSAGES_NUM; i++) {
					ctx.collect("amq-" + i);
				}
			}

			@Override
			public void cancel() {}
		});


		ActiveMQConnectionFactory connectionFactory = createConnectionFactory();
		AMQSink<String> sink = new AMQSink<>(
			connectionFactory,
			QUEUE_NAME,
			new SimpleStringSchema()
		);


		stream.addSink(sink);
	}

	private ActiveMQConnectionFactory createConnectionFactory() {
		return new ActiveMQConnectionFactory("vm://localhost?broker.persistent=false");
	}

	private void createConsumerTopology(StreamExecutionEnvironment env) {
		ActiveMQConnectionFactory sourceConnectionFactory = createConnectionFactory();
		AMQSource<String> source = new AMQSource<>(
			sourceConnectionFactory,
			QUEUE_NAME,
			new SimpleStringSchema()
		);


		env.addSource(source)
			.addSink(new SinkFunction<String>() {
				final HashSet<Integer> set = new HashSet<>();
				@Override
				public void invoke(String value) throws Exception {
					int val = Integer.parseInt(value.split("-")[1]);
					set.add(val);

					if (set.size() == MESSAGES_NUM) {
						throw new SuccessException();
					}
				}
			});
	}
}
