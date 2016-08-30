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
 * flink-siddhi integration test suites
 */
public class SiddhiStreamITSuite extends StreamingMultipleProgramsTestBase {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testSimpleAcceptPOJOAndReturnPojo() throws Exception {
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
			.inject("inputStream", input, "id", "name", "price")
			.apply("from inputStream insert into  outputStream")
			.returns("outputStream", Event.class);
		String path = tempFolder.newFile().toURI().toString();
		output.writeAsText(path);
		env.execute();
	}

	@Test
	public void testSimpleAcceptPOJOAndReturnMap() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setBufferTimeout(5000);

		DataStream<Event> input = env.fromElements(
			Event.of(1, "start", 1.0),
			Event.of(2, "middle", 2.0),
			Event.of(3, "end", 3.0),
			Event.of(4, "start", 4.0),
			Event.of(5, "middle", 5.0)
		);

		DataStream<Map<String, Object>> output = SiddhiStream
			.inject("inputStream", input, "id", "name", "price")
			.apply("from inputStream insert into  outputStream")
			.returns("outputStream");
		output.print();
		env.execute();
	}

	@Test
	public void testUnboundedPOJOSourceAndReturnMap() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		env.setBufferTimeout(5000);
		DataStream<Event> input = env.addSource(new EventSource(5));

		DataStream<Map<String, Object>> output = SiddhiStream
			.inject("inputStream", input, "id", "name", "price")
			.apply("from inputStream insert into  outputStream")
			.returns("outputStream");

		output.printToErr();

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}

	@Test
	public void testUnboundedPOJOSourceAndReturnPOJO() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		DataStream<Event> input = env.addSource(new EventSource(5));

		SiddhiStream siddhiStream = SiddhiStream.newSiddhiStream(env);
		siddhiStream.register("inputStream", input, "id", "name", "price");

		DataStream<Event> output = siddhiStream
			.apply("from inputStream insert into  outputStream")
			.returns("outputStream", Event.class);

		output.printToErr();

		String resultPath = tempFolder.newFile().toURI().toString();
		output.writeAsText(resultPath, FileSystem.WriteMode.OVERWRITE);
		env.execute();
		assertEquals(5, getLineCount(resultPath));
	}

	public static int getLineCount(String resPath) throws IOException {
		List<String> result = new LinkedList<>();
		readAllResultLines(result, resPath);
		return result.size();
	}
}

