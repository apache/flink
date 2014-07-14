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

package org.apache.flink.streaming.api;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.java.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.function.SinkFunction;
import org.apache.flink.util.Collector;
import org.junit.Test;

public class IterateTest {

	private static final long MEMORYSIZE = 32;
	private static boolean iterated = false;

	public static final class IterationHead extends
			FlatMapFunction<Tuple1<Boolean>, Tuple1<Boolean>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple1<Boolean> value,
				Collector<Tuple1<Boolean>> out) throws Exception {
			if (value.f0) {
				iterated = true;
			}else{
				out.collect(value);
			}

		}

	}

	public static final class IterationTail extends
			FlatMapFunction<Tuple1<Boolean>, Tuple1<Boolean>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Tuple1<Boolean> value,
				Collector<Tuple1<Boolean>> out) throws Exception {
			out.collect(new Tuple1<Boolean>(true));

		}

	}
	
	public static final class MySink extends SinkFunction<Tuple1<Boolean>> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Tuple1<Boolean> tuple) {
		}
		
	}
	@Test
	public void test() throws Exception {

		LocalStreamEnvironment env = StreamExecutionEnvironment
				.createLocalEnvironment(1);
		
		List<Boolean> bl = new ArrayList<Boolean>();
		for(int i =0;i<1000000;i++){
			bl.add(false);
		}

		IterativeDataStream<Tuple1<Boolean>> source = env.fromCollection(bl).flatMap(new IterationHead()).
				iterate();
		
		DataStream<Tuple1<Boolean>> increment = source.flatMap(new IterationTail());
		
		source.closeWith(increment).addSink(new MySink());

		env.executeTest(MEMORYSIZE);
		
		assertTrue(iterated);

	}

}
