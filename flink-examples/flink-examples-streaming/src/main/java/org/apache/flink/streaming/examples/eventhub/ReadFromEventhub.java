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

package org.apache.flink.streaming.examples.eventhub;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.eventhubs.FlinkEventHubConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

/**
 * Created by jozh on 6/21/2017.
 */
public class ReadFromEventhub {
	public static void main(String[] args) throws Exception {
		// parse input arguments
		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() < 4) {
			System.out.println("Missing parameters!\nUsage: ReadFromEventhub --eventhubs.policykey <key> " +
				"--eventhubs.namespace <namespace> --eventhubs.name <name> --eventhubs.partition.count <count>");
			return;
		}

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableSysoutLogging();
		env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
		env.enableCheckpointing(5000); // create a checkpoint every 5 seconds
		env.getConfig().setGlobalJobParameters(parameterTool); // make parameters available in the web interface

		DataStream<String> messageStream = env
			.addSource(new FlinkEventHubConsumer<String>(
				parameterTool.getProperties(),
				new SimpleStringSchema()));

		// write kafka stream to standard out.
		messageStream.print();

		env.execute("Read from eventhub example");
	}
}
