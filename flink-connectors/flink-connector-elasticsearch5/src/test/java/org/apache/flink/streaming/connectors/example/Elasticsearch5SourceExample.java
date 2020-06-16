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

package org.apache.flink.streaming.connectors.example;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticSearch5InputFormat;
import org.apache.flink.types.Row;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * End to end test for Elasticsearch5Sink.
 */
public class Elasticsearch5SourceExample {

	public static void main(String[] args) throws Exception {

		final ParameterTool parameterTool = ParameterTool.fromArgs(args);

		if (parameterTool.getNumberOfParameters() != 2) {
			System.out.println("Missing parameters!\n" +
				"Usage: --index <index> --type <type>");
			return;
		}

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);

		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("10.3.69.181"), 9300));

		RowTypeInfo rowTypeInfo = new RowTypeInfo(
			new TypeInformation[]{Types.LONG, Types.STRING},
			new String[]{"some_long", "some_string"});

		Map<String, String> userConfig = new HashMap<>();
		userConfig.put("cluster.name", "SERVICE-ELASTICSEARCH-retro-1");
		userConfig.put("transport.tcp.connect_timeout", "5s");

		DeserializationSchema<Row> deserializationSchema = new JsonRowDeserializationSchema(rowTypeInfo);
		ElasticSearch5InputFormat elasticSearch5InputFormat = new ElasticSearch5InputFormat.Builder()
			.setTransportAddresses(transports)
			.setUserConfig(userConfig)
			.setScrollMaxSize(100)
			.setScrollTimeout(10000)
			.setFieldNames(rowTypeInfo.getFieldNames())
//			.setFieldNames(new String[]{"some_string", "some_long"})
//			.setRowDataTypeInfo(rowTypeInfo)
			.setDeserializationSchema(deserializationSchema)
			.setIndex(parameterTool.getRequired("index"))
			.setType(parameterTool.getRequired("type"))
			.build();

		DataStream<RowTypeInfo> dataStream = env.createInput(elasticSearch5InputFormat);
		dataStream.print();

		env.execute("Elasticsearch5.x end to end sink test example");
	}

}
