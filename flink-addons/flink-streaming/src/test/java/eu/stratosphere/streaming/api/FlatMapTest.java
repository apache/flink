package eu.stratosphere.streaming.api;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;

import org.junit.Test;

import eu.stratosphere.api.datastream.DataStream;
import eu.stratosphere.api.datastream.SinkFunction;
import eu.stratosphere.api.datastream.StreamExecutionEnvironment;
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

		@Override
		public void invoke(Tuple1<String> tuple) {
			// TODO Auto-generated method stub
			System.out.println(tuple);
		}

	}

	@Test
	public void test() throws Exception {
		Tuple1<String> tup = new Tuple1<String>("asd");

		StreamExecutionEnvironment context = new StreamExecutionEnvironment();
		DataStream<Tuple1<String>> dataStream0 = context.setDummySource();

		DataStream<Tuple1<String>> dataStream1 = context.setDummySource().connectWith(dataStream0)
				.partitionBy(0).flatMap(new MyFlatMap()).broadcast().addSink(new MySink());

		 context.execute();

		JobGraphBuilder jgb = context.jobGB();

		for (AbstractJobVertex c : jgb.components.values()) {
			if (c instanceof JobTaskVertex) {
				Configuration config = c.getConfiguration();
				System.out.println(config.getString("componentName", "default"));
				byte[] bytes = config.getBytes("operator", null);

				ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));

				FlatMapFunction<Tuple, Tuple> f = (FlatMapFunction<Tuple, Tuple>) in.readObject();

				StreamCollector<Tuple> s = new StreamCollector<Tuple>(1, 1, null);
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
