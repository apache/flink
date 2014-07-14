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
import eu.stratosphere.streaming.api.invokable.UserSinkInvokable;
import eu.stratosphere.streaming.api.invokable.UserSourceInvokable;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.util.Collector;

public class FlatMapTest {

	public static final class MyFlatMap extends FlatMapFunction<Tuple1<String>, Tuple1<String>> {
		@Override
		public void flatMap(Tuple1<String> value, Collector<Tuple1<String>> out) throws Exception {
			out.collect(value);
			System.out.println("flatMap");
		}
	}

	public static final class MySink extends SinkFunction<Tuple1<String>> {
		int c=0;
		@Override
		public void invoke(Tuple1<String> tuple) {
			System.out.println(tuple);
			c++;
			System.out.println(c);
		}

	}

	public static final class MySource extends SourceFunction<Tuple1<String>> {

		@Override
		public void invoke(Collector<Tuple1<String>> collector) {
			for (int i = 0; i < 5; i++) {
				collector.collect(new Tuple1<String>("hi"));
			}
		}
	}

	private static final int PARALELISM = 2;

	@Test
	public void test() throws Exception {

		try {
			StreamExecutionEnvironment env2 = new StreamExecutionEnvironment(0, 1000);
			fail();
		} catch (IllegalArgumentException e) {
			try {
				StreamExecutionEnvironment env2 = new StreamExecutionEnvironment(1, 0);
				fail();
			} catch (IllegalArgumentException e2) {	
			}
		}
		
		StreamExecutionEnvironment env = new StreamExecutionEnvironment(2, 1000);
		DataStream<Tuple1<String>> dataStream0 = env.addSource(new MySource(),1);

		DataStream<Tuple1<String>> dataStream1 = env.addDummySource().connectWith(dataStream0)
				.partitionBy(0).flatMap(new MyFlatMap(), PARALELISM).broadcast().addSink(new MySink());

		env.execute();

		JobGraphBuilder jgb = env.jobGB();

		for (AbstractJobVertex c : jgb.components.values()) {
			if (c instanceof JobTaskVertex) {
				Configuration config = c.getConfiguration();
				System.out.println(config.getString("componentName", "default"));
				byte[] bytes = config.getBytes("operator", null);

				ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));

				FlatMapFunction<Tuple, Tuple> f = (FlatMapFunction<Tuple, Tuple>) in.readObject();

				StreamCollector<Tuple> s = new StreamCollector<Tuple>(1, 1000, 1, null);
				Tuple t = new Tuple1<String>("asd");

				f.flatMap(t, s);

				System.out.println(f.getClass().getGenericSuperclass());
				TupleTypeInfo<Tuple> ts = (TupleTypeInfo) TypeExtractor.createTypeInfo(
						FlatMapFunction.class, f.getClass(), 0, null, null);

				System.out.println(ts);

				byte[] userFunctionSerialized = config.getBytes("serializedudf", null);
				in = new ObjectInputStream(new ByteArrayInputStream(userFunctionSerialized));
				UserTaskInvokable userFunction = (UserTaskInvokable) in.readObject();
				System.out.println(userFunction.getClass());
				assertTrue(true);
				System.out.println("----------------");
			}

			if (c instanceof JobOutputVertex) {
				Configuration config = c.getConfiguration();
				System.out.println(config.getString("componentName", "default"));
				byte[] bytes = config.getBytes("operator", null);

				ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));

				SinkFunction<Tuple> f = (SinkFunction<Tuple>) in.readObject();

				System.out.println(f.getClass().getGenericSuperclass());
				TupleTypeInfo<Tuple> ts = (TupleTypeInfo) TypeExtractor.createTypeInfo(
						SinkFunction.class, f.getClass(), 0, null, null);

				System.out.println(ts);

				byte[] userFunctionSerialized = config.getBytes("serializedudf", null);
				in = new ObjectInputStream(new ByteArrayInputStream(userFunctionSerialized));
				UserSinkInvokable userFunction = (UserSinkInvokable) in.readObject();
				System.out.println(userFunction.getClass());
				assertTrue(true);
				System.out.println("----------------");
			}

			if (c instanceof JobInputVertex) {
				Configuration config = c.getConfiguration();
				System.out.println(config.getString("componentName", "default"));
				byte[] bytes = config.getBytes("operator", null);

				ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));

				UserSourceInvokable<Tuple> f = (UserSourceInvokable<Tuple>) in.readObject();

				System.out.println(f.getClass().getGenericSuperclass());
				TupleTypeInfo<Tuple> ts = (TupleTypeInfo) TypeExtractor.createTypeInfo(
						UserSourceInvokable.class, f.getClass(), 0, null, null);

				System.out.println(ts);
				System.out.println("----------------");
			}
		}
	}
}
