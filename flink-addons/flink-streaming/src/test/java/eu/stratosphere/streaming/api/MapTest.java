/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.api;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;

import eu.stratosphere.util.Collector;

public class MapTest {

	public static final class MySource extends SourceFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector)
				throws Exception {
			for(int i=0; i<10; i++){
				collector.collect(new Tuple1<Integer>(i));
			}
		}
	}
	
	public static final class MyMap extends MapFunction<Tuple1<Integer>,Tuple1<Integer>> {

		@Override
		public Tuple1<Integer> map(Tuple1<Integer> value) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple1<Integer>(value.f0*value.f0);
		}
	}
	
	public static final class MySink extends SinkFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Tuple1<Integer> tuple) {
			result.add(tuple.f0);
			System.out.println("result " + tuple.f0);
		}
	}

	private static List<Integer> expected = new ArrayList<Integer>();
	private static List<Integer> result = new ArrayList<Integer>();
	private static final int PARALELISM = 1;

	private static void fillExpectedList(){
		for(int i=0;i<10;i++){
			expected.add(i*i);
			System.out.println("expected " + i*i);
		}
	}
	
	@Test
	public void test() throws Exception {
		
		StreamExecutionEnvironment context = new StreamExecutionEnvironment();

		DataStream<Tuple1<Integer>> dataStream = context
				.addSource(new MySource(), 1)
				.map(new MyMap(), PARALELISM)
				.addSink(new MySink());

		context.execute();
		
		fillExpectedList();

		assertTrue(expected.equals(result));
	}
}
