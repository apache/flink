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
package org.apache.flink.streaming.connectors.rethinkdb.integration;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.rethinkdb.FlinkRethinkDbSink;
import org.apache.flink.streaming.connectors.rethinkdb.StringJSONSerializationSchema;

/**
 * This is a integration for RethinkDB sink.  For this to run, we need an instance of 
 * RethinkDB running and set the parameters to connect to it.  The code instantiates
 * two json event generators - one which generates <code>id</code> field and another without <code>id</code>.
 */
public class RethinkDBITCase {

	public static void main(String[] args) throws Exception {
		
		ParameterTool parameterTool = ParameterTool.fromArgs(args);
		String hostname = parameterTool.get("hostname", "localhost");
		int hostport = parameterTool.getInt("hostport", 28015);
		String dbname = parameterTool.get("dbname", "test");
		String table = parameterTool.get("table", "JsonTestTable");

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.setParallelism(1);

		DataStream<String> stringStreamWithId = 
				see.addSource(new JSONEventGenerator(true));
		
		DataStream<String> stringStreamWithOutId = 
				see.addSource(new JSONEventGenerator(false));

		FlinkRethinkDbSink<String> sink = new FlinkRethinkDbSink<String>(
				hostname, hostport, dbname, table,
				new StringJSONSerializationSchema());

		stringStreamWithId.addSink(sink);
		stringStreamWithOutId.addSink(sink);
		
		see.execute();
	}

}
