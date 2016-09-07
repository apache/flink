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

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.contrib.siddhi.extension.CustomPlusFunctionExtension;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
 * flink-siddhi integration test cases
 */
public class SiddhiStreamITCase extends StreamingMultipleProgramsTestBase {

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

		DataStream<Event> output = SiddhiStream
			.from("inputStream", input, "id", "name", "price")
			.query("from inputStream insert into  outputStream")
			.returns("outputStream", Event.class);
		String path = tempFolder.newFile().toURI().toString();
		output.writeAsText(path);
		env.execute();
	}

	@Test
	public void testUnboundedPojoSourceAndReturnTuple() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input = env.addSource(new RandomEventSource(5));

		DataStream<Tuple4<Long,Integer,String,Double>> output = SiddhiStream
			.from("inputStream", input, "id", "name", "price","timestamp")
			.query("from inputStream select timestamp, id, name, price insert into  outputStream")
			.returns("outputStream");

		output.printToErr();

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}

	@Test
	public void testUnboundedPojoStreamAndReturnMap() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setBufferTimeout(5000);
		DataStream<Event> input = env.addSource(new RandomEventSource(5));

		DataStream<Map> output = SiddhiStream
			.from("inputStream", input, "id", "name", "price","timestamp")
			.query("from inputStream select timestamp, id, name, price insert into  outputStream")
			.returnAsMap("outputStream");

		output.printToErr();

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}

	@Test
	public void testUnboundedPojoStreamAndReturnPojo() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input = env.addSource(new RandomEventSource(5));

		DataStream<Event> output = SiddhiStream
			.from("inputStream", input, "id", "name", "price","timestamp")
			.query("from inputStream select timestamp, id, name, price insert into  outputStream")
			.returns("outputStream",Event.class);

		output.printToErr();

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}


	@Test
	public void testMultipleUnboundedPojoStreamSimpleUnion() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input1 = env.addSource(new RandomEventSource(5),"input1");
		DataStream<Event> input2 = env.addSource(new RandomEventSource(5),"input2");
		DataStream<Event> input3 = env.addSource(new RandomEventSource(5),"input2");
		DataStream<Event> output = SiddhiStream
			.from("inputStream1", input1, "id", "name", "price","timestamp")
			.union("inputStream2", input2, "id", "name", "price","timestamp")
			.union("inputStream3", input3, "id", "name", "price","timestamp")
			.query(
				"from inputStream1 select timestamp, id, name, price insert into outputStream;"
				+ "from inputStream2 select timestamp, id, name, price insert into outputStream;"
				+ "from inputStream3 select timestamp, id, name, price insert into outputStream;"
			)
			.returns("outputStream",Event.class);

		output.printToErr();

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(15, getLineCount(resultPath));
	}

	/**
	 * @see <a href="https://docs.wso2.com/display/CEP300/Joins">https://docs.wso2.com/display/CEP300/Joins</a>
     */
	@Test
	public void testMultipleUnboundedPojoStreamUnionAndJoinWithWindow() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input1 = env.addSource(new RandomEventSource(5),"input1");
		DataStream<Event> input2 = env.addSource(new RandomEventSource(5),"input2");

		DataStream<Map> output = SiddhiStream
			.from("inputStream1", input1.keyBy("id"), "id", "name", "price","timestamp")
			.union("inputStream2", input2.keyBy("id"), "id", "name", "price","timestamp")
			.query(
				"from inputStream1#window.length(5) as s1 "
				+ "join inputStream2#window.time(500) as s2 "
				+ "on s1.id == s2.id "
				+ "select s1.timestamp as t, s1.name as n, s1.price as p1, s2.price as p2 "
				+ "insert into JoinStream;"
			)
			.returnAsMap("JoinStream");

		output.printToErr();

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
		DataStream<Event> input1 = env.addSource(new RandomEventSource(5),"input1");
		DataStream<Event> input2 = env.addSource(new RandomEventSource(5),"input2");

		DataStream<Map> output = SiddhiStream
			.from("inputStream1", input1.keyBy("name"), "id", "name", "price","timestamp")
			.union("inputStream2", input2.keyBy("name"), "id", "name", "price","timestamp")
			.query(
				"from every s1 = inputStream1[id == 2] "
				+ " -> s2 = inputStream2[id == 3] "
				+ "select s1.id as id_1, s1.name as name_1, s2.id as id_2, s2.name as name_2 "
				+ "insert into outputStream"
			)
			.returnAsMap("outputStream");

		output.printToErr();

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(1, getLineCount(resultPath));
		compareResultsByLinesInMemory("{id_1=2, name_1=test_event, id_2=3, name_2=test_event}",resultPath);
	}

	/**
	 * @see <a href="https://docs.wso2.com/display/CEP300/Joins">https://docs.wso2.com/display/CEP300/Sequences</a>
	 */
	@Test
	public void testUnboundedPojoStreamSimpleSequences() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input1 = env.addSource(new RandomEventSource(5),"input1");
		
		DataStream<Map> output = SiddhiStream
			.from("inputStream1", input1.keyBy("name"), "id", "name", "price","timestamp")
			.union("inputStream2", input1.keyBy("name"), "id", "name", "price","timestamp")
			.query(
				"from every s1 = inputStream1[id == 2]+ , "
				+ "s2 = inputStream2[id == 3]? "
				+ "within 1000 second "
				+ "select s1[0].name as n1, s2.name as n2 "
				+ "insert into outputStream"
			)
			.returnAsMap("outputStream");

		output.printToErr();

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(1, getLineCount(resultPath));
	}

	public static int getLineCount(String resPath) throws IOException {
		List<String> result = new LinkedList<>();
		readAllResultLines(result, resPath);
		return result.size();
	}

	@Test
	public void testSiddhiFunctionExtension() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Event> input = env.addSource(new RandomEventSource(5));

		SiddhiStream.registerExtension("custom:plus",CustomPlusFunctionExtension.class);

		DataStream<Map> output = SiddhiStream
			.from("inputStream", input, "id", "name", "price","timestamp")
			.query("from inputStream select timestamp, id, name, custom:plus(price,price) as doubled_price insert into  outputStream")
			.returnAsMap("outputStream");

		output.printToErr();

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}
}
