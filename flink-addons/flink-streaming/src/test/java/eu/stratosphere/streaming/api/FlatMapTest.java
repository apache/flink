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
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobInputVertex;
import eu.stratosphere.nephele.jobgraph.JobOutputVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.streaming.api.MapTest.MyMap;
import eu.stratosphere.streaming.api.MapTest.MySink;
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.util.Collector;

public class FlatMapTest {

	public static final class MyFlatMap extends FlatMapFunction<Tuple1<Integer>, Tuple1<Integer>> {

		@Override
		public void flatMap(Tuple1<Integer> value,
				Collector<Tuple1<Integer>> out) throws Exception {
			out.collect(new Tuple1<Integer>(value.f0*value.f0));
			
		}
		
	}

	public static final class MySink extends SinkFunction<Tuple1<Integer>> {
		
		@Override
		public void invoke(Tuple1<Integer> tuple) {
			result.add(tuple.f0);
			System.out.println("result " + tuple.f0);
		}

	}

	public static final class MySource extends SourceFunction<Tuple1<Integer>> {

		@Override
		public void invoke(Collector<Tuple1<Integer>> collector)
				throws Exception {
			for(int i=0; i<10; i++){
				collector.collect(new Tuple1<Integer>(i));
			}
		}
	}
	
	private static void fillExpectedList(){
		for(int i=0;i<10;i++){
			expected.add(i*i);
		}
	}

	private static final int PARALELISM = 1;
	private static List<Integer> expected = new ArrayList<Integer>();
	private static List<Integer> result = new ArrayList<Integer>();

	@Test
	public void test() throws Exception {
		
		StreamExecutionEnvironment env = new StreamExecutionEnvironment(2, 1000);
		DataStream<Tuple1<Integer>> dataStream = env.addSource(new MySource(),1).flatMap(new MyFlatMap(), PARALELISM).addSink(new MySink());


		env.execute();
		
		fillExpectedList();
		
		assertTrue(expected.equals(result));
		
	}
}
