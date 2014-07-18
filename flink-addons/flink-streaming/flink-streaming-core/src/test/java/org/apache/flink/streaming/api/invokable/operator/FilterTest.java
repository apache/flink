/**
 *
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
 *
 */

package org.apache.flink.streaming.api.invokable.operator;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

import org.apache.flink.api.java.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.util.LogUtils;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

public class FilterTest implements Serializable {
	private static final long serialVersionUID = 1L;
	
	private static Set<Integer> set = new HashSet<Integer>();

	private static class SetSink extends SinkFunction<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple1<Integer> tuple) {
			set.add(tuple.f0);
		}
	}

	static class MyFilter extends FilterFunction<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(Tuple1<Integer> value) throws Exception {
			return value.f0 % 2 == 0;
		}
	}

	@Test
	public void test() {
		LogUtils.initializeDefaultConsoleLogger(Level.OFF, Level.OFF);
		
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		env.fromElements(1, 2, 3, 4, 5, 6, 7).filter(new MyFilter()).addSink(new SetSink());

		env.execute();
		
		Assert.assertArrayEquals(new Integer[] { 2, 4, 6 }, set.toArray());
	}
}
