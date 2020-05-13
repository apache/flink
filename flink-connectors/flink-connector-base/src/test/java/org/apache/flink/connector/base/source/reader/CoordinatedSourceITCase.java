/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.common.accumulators.ListAccumulator;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.mocks.MockBaseSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

/**
 * IT case for the {@link Source} with a coordinator.
 */
public class CoordinatedSourceITCase extends AbstractTestBase {

	@Test
	public void testEnumeratorReaderCommunication() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		MockBaseSource source = new MockBaseSource(2, 10, Boundedness.BOUNDED);
		DataStream<Integer> stream = env.continuousSource(source, "TestingSource");
		stream.addSink(new RichSinkFunction<Integer>() {
			@Override
			public void open(Configuration parameters) throws Exception {
				getRuntimeContext().addAccumulator("result", new ListAccumulator<Integer>());
			}

			@Override
			public void invoke(Integer value, Context context) throws Exception {
				getRuntimeContext().getAccumulator("result").add(value);
			}
		});
		List<Integer> result = env.execute().getAccumulatorResult("result");
		SortedSet<Integer> resultSet = new TreeSet<>(result);
		assertEquals(0, (int) resultSet.first());
		assertEquals(19, (int) resultSet.last());
		assertEquals(20, resultSet.size());
	}

}
