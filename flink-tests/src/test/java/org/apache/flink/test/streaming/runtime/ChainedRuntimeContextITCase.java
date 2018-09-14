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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Test;

import static org.junit.Assert.assertNotEquals;

/**
 * Test creation of context for chained streaming operators.
 */
@SuppressWarnings("serial")
public class ChainedRuntimeContextITCase extends AbstractTestBase {
	private static RuntimeContext srcContext;
	private static RuntimeContext mapContext;

	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env.addSource(new TestSource()).map(new TestMap()).addSink(new DiscardingSink<Integer>());
		env.execute();

		assertNotEquals(srcContext, mapContext);

	}

	private static class TestSource extends RichParallelSourceFunction<Integer> {

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
		}

		@Override
		public void cancel() {
		}

		@Override
		public void open(Configuration c) {
			srcContext = getRuntimeContext();
		}

	}

	private static class TestMap extends RichMapFunction<Integer, Integer> {

		@Override
		public Integer map(Integer value) throws Exception {
			return value;
		}

		@Override
		public void open(Configuration c) {
			mapContext = getRuntimeContext();
		}

	}

}
