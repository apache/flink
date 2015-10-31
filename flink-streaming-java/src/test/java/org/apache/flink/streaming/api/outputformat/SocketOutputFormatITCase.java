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

package org.apache.flink.streaming.api.outputformat;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.util.SocketOutputTestBase;
import org.apache.flink.test.testdata.WordCountData;
import org.junit.Ignore;

@Ignore
//This test sometimes failes most likely due to the behaviour
//of the socket. Disabled for now.
public class SocketOutputFormatITCase extends SocketOutputTestBase {

	@Override
	protected void testProgram() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> text = env.fromElements(WordCountData.TEXT);

		DataStream<String> counts =
				text.flatMap(new CsvOutputFormatITCase.Tokenizer())
						.keyBy(0).sum(1).map(new MapFunction<Tuple2<String, Integer>, String>() {
					@Override
					public String map(Tuple2<String, Integer> value) throws Exception {
						return value.toString() + "\n";
					}
				});
		counts.writeToSocket(HOST, port, new DummyStringSchema());

		env.execute("WriteToSocketTest");
	}

}
