/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi;

import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.contrib.siddhi.exception.UndefinedStreamException;
import org.apache.flink.contrib.siddhi.extension.CustomPlusFunctionExtension;
import org.apache.flink.contrib.siddhi.source.Event;
import org.apache.flink.contrib.siddhi.source.RandomEventSource;
import org.apache.flink.contrib.siddhi.source.RandomTupleSource;
import org.apache.flink.contrib.siddhi.source.RandomWordSource;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Flink-siddhi library integration test cases
 */
public class SiddhiCEPITCase extends StreamingMultipleProgramsTestBase {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testSimplePojoStreamAndReturnPojo() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input = env.fromElements(
			Event.of(1, "start", 1.0),
			Event.of(2, "middle", 2.0),
			Event.of(3, "end", 3.0),
			Event.of(4, "start", 4.0),
			Event.of(5, "middle", 5.0),
			Event.of(6, "end", 6.0)
		);

		DataStream<Event> output = SiddhiCEP
			.define("inputStream", input, "id", "name", "price")
			.cql("from inputStream insert into  outputStream")
			.returns("outputStream", Event.class);
		String path = tempFolder.newFile().toURI().toString();
		output.writeAsText(path);
		env.execute();
	}

	@Test
	public void testUnboundedPojoSourceAndReturnTuple() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input = env.addSource(new RandomEventSource(5));

		DataStream<Tuple4<Long, Integer, String, Double>> output = SiddhiCEP
			.define("inputStream", input, "id", "name", "price", "timestamp")
			.cql("from inputStream select timestamp, id, name, price insert into  outputStream")
			.returns("outputStream");

		DataStream<Integer> following = output.map(new MapFunction<Tuple4<Long, Integer, String, Double>, Integer>() {
			@Override
			public Integer map(Tuple4<Long, Integer, String, Double> value) throws Exception {
				return value.f1;
			}
		});
		String resultPath = tempFolder.newFile().toURI().toString();
		following.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}

	@Test
	public void testUnboundedTupleSourceAndReturnTuple() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Tuple4<Integer, String, Double, Long>> input = env.addSource(new RandomTupleSource(5).closeDelay(1500));

		DataStream<Tuple4<Long, Integer, String, Double>> output = SiddhiCEP
			.define("inputStream", input, "id", "name", "price", "timestamp")
			.cql("from inputStream select timestamp, id, name, price insert into  outputStream")
			.returns("outputStream");

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}

	@Test
	public void testUnboundedPrimitiveTypeSourceAndReturnTuple() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<String> input = env.addSource(new RandomWordSource(5).closeDelay(1500));

		DataStream<Tuple1<String>> output = SiddhiCEP
			.define("wordStream", input, "words")
			.cql("from wordStream select words insert into  outputStream")
			.returns("outputStream");

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}

	@Test(expected = InvalidTypesException.class)
	public void testUnboundedPojoSourceButReturnInvalidTupleType() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input = env.addSource(new RandomEventSource(5).closeDelay(1500));

		DataStream<Tuple5<Long, Integer, String, Double, Long>> output = SiddhiCEP
			.define("inputStream", input, "id", "name", "price", "timestamp")
			.cql("from inputStream select timestamp, id, name, price insert into  outputStream")
			.returns("outputStream");

		DataStream<Long> following = output.map(new MapFunction<Tuple5<Long, Integer, String, Double, Long>, Long>() {
			@Override
			public Long map(Tuple5<Long, Integer, String, Double, Long> value) throws Exception {
				return value.f0;
			}
		});

		String resultPath = tempFolder.newFile().toURI().toString();
		following.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
		env.execute();
	}

	@Test
	public void testUnboundedPojoStreamAndReturnMap() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		DataStream<Event> input = env.addSource(new RandomEventSource(5));

		DataStream<Map<String, Object>> output = SiddhiCEP
			.define("inputStream", input, "id", "name", "price", "timestamp")
			.cql("from inputStream select timestamp, id, name, price insert into  outputStream")
			.returnAsMap("outputStream");

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}

	@Test
	public void testUnboundedPojoStreamAndReturnPojo() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input = env.addSource(new RandomEventSource(5));
		input.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Event>() {
			@Override
			public long extractAscendingTimestamp(Event element) {
				return element.getTimestamp();
			}
		});

		DataStream<Event> output = SiddhiCEP
			.define("inputStream", input, "id", "name", "price", "timestamp")
			.cql("from inputStream select timestamp, id, name, price insert into  outputStream")
			.returns("outputStream", Event.class);

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}


	@Test
	public void testMultipleUnboundedPojoStreamSimpleUnion() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input1 = env.addSource(new RandomEventSource(2), "input1");
		DataStream<Event> input2 = env.addSource(new RandomEventSource(2), "input2");
		DataStream<Event> input3 = env.addSource(new RandomEventSource(2), "input2");
		DataStream<Event> output = SiddhiCEP
			.define("inputStream1", input1, "id", "name", "price", "timestamp")
			.union("inputStream2", input2, "id", "name", "price", "timestamp")
			.union("inputStream3", input3, "id", "name", "price", "timestamp")
			.cql(
				"from inputStream1 select timestamp, id, name, price insert into outputStream;"
					+ "from inputStream2 select timestamp, id, name, price insert into outputStream;"
					+ "from inputStream3 select timestamp, id, name, price insert into outputStream;"
			)
			.returns("outputStream", Event.class);

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(6, getLineCount(resultPath));
	}

	/**
	 * @see <a href="https://docs.wso2.com/display/CEP300/Joins">https://docs.wso2.com/display/CEP300/Joins</a>
	 */
	@Test
	public void testMultipleUnboundedPojoStreamUnionAndJoinWithWindow() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input1 = env.addSource(new RandomEventSource(5), "input1");
		DataStream<Event> input2 = env.addSource(new RandomEventSource(5), "input2");

		DataStream<? extends Map> output = SiddhiCEP
			.define("inputStream1", input1.keyBy("id"), "id", "name", "price", "timestamp")
			.union("inputStream2", input2.keyBy("id"), "id", "name", "price", "timestamp")
			.cql(
				"from inputStream1#window.length(5) as s1 "
					+ "join inputStream2#window.time(500) as s2 "
					+ "on s1.id == s2.id "
					+ "select s1.timestamp as t, s1.name as n, s1.price as p1, s2.price as p2 "
					+ "insert into JoinStream;"
			)
			.returnAsMap("JoinStream");

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}

	/**
	 * @see <a href="https://docs.wso2.com/display/CEP300/Joins">https://docs.wso2.com/display/CEP300/Patterns</a>
	 */
	@Test
	public void testUnboundedPojoStreamSimplePatternMatch() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input1 = env.addSource(new RandomEventSource(5).closeDelay(1500), "input1");
		DataStream<Event> input2 = env.addSource(new RandomEventSource(5).closeDelay(1500), "input2");

		DataStream<Map<String, Object>> output = SiddhiCEP
			.define("inputStream1", input1.keyBy("name"), "id", "name", "price", "timestamp")
			.union("inputStream2", input2.keyBy("name"), "id", "name", "price", "timestamp")
			.cql(
				"from every s1 = inputStream1[id == 2] "
					+ " -> s2 = inputStream2[id == 3] "
					+ "select s1.id as id_1, s1.name as name_1, s2.id as id_2, s2.name as name_2 "
					+ "insert into outputStream"
			)
			.returnAsMap("outputStream");

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(1, getLineCount(resultPath));
		compareResultsByLinesInMemory("{id_1=2, name_1=test_event, id_2=3, name_2=test_event}", resultPath);
	}

	/**
	 * @see <a href="https://docs.wso2.com/display/CEP300/Joins">https://docs.wso2.com/display/CEP300/Sequences</a>
	 */
	@Test
	public void testUnboundedPojoStreamSimpleSequences() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input1 = env.addSource(new RandomEventSource(5).closeDelay(1500), "input1");
		DataStream<Map<String, Object>> output = SiddhiCEP
			.define("inputStream1", input1.keyBy("name"), "id", "name", "price", "timestamp")
			.union("inputStream2", input1.keyBy("name"), "id", "name", "price", "timestamp")
			.cql(
				"from every s1 = inputStream1[id == 2]+ , "
					+ "s2 = inputStream2[id == 3]? "
					+ "within 1000 second "
					+ "select s1[0].name as n1, s2.name as n2 "
					+ "insert into outputStream"
			)
			.returnAsMap("outputStream");

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(1, getLineCount(resultPath));
	}

	private static int getLineCount(String resPath) throws IOException {
		List<String> result = new LinkedList<>();
		readAllResultLines(result, resPath);
		return result.size();
	}

	@Test
	public void testCustomizeSiddhiFunctionExtension() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input = env.addSource(new RandomEventSource(5));

		SiddhiCEP cep = SiddhiCEP.getSiddhiEnvironment(env);
		cep.registerExtension("custom:plus", CustomPlusFunctionExtension.class);

		DataStream<Map<String, Object>> output = cep
			.from("inputStream", input, "id", "name", "price", "timestamp")
			.cql("from inputStream select timestamp, id, name, custom:plus(price,price) as doubled_price insert into  outputStream")
			.returnAsMap("outputStream");

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}

	@Test
	public void testRegisterStreamAndExtensionWithSiddhiCEPEnvironment() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input1 = env.addSource(new RandomEventSource(5), "input1");
		DataStream<Event> input2 = env.addSource(new RandomEventSource(5), "input2");

		SiddhiCEP cep = SiddhiCEP.getSiddhiEnvironment(env);
		cep.registerExtension("custom:plus", CustomPlusFunctionExtension.class);

		cep.registerStream("inputStream1", input1.keyBy("id"), "id", "name", "price", "timestamp");
		cep.registerStream("inputStream2", input2.keyBy("id"), "id", "name", "price", "timestamp");

		DataStream<Tuple4<Long, String, Double, Double>> output = cep
			.from("inputStream1").union("inputStream2")
			.cql(
				"from inputStream1#window.length(5) as s1 "
					+ "join inputStream2#window.time(500) as s2 "
					+ "on s1.id == s2.id "
					+ "select s1.timestamp as t, s1.name as n, s1.price as p1, s2.price as p2 "
					+ "insert into JoinStream;"
			)
			.returns("JoinStream");

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}

	@Test(expected = UndefinedStreamException.class)
	public void testTriggerUndefinedStreamException() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input1 = env.addSource(new RandomEventSource(5), "input1");

		SiddhiCEP cep = SiddhiCEP.getSiddhiEnvironment(env);
		cep.registerStream("inputStream1", input1.keyBy("id"), "id", "name", "price", "timestamp");

		DataStream<Map<String, Object>> output = cep
			.from("inputStream1").union("inputStream2")
			.cql(
				"from inputStream1#window.length(5) as s1 "
					+ "join inputStream2#window.time(500) as s2 "
					+ "on s1.id == s2.id "
					+ "select s1.timestamp as t, s1.name as n, s1.price as p1, s2.price as p2 "
					+ "insert into JoinStream;"
			)
			.returnAsMap("JoinStream");

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
	}
}
