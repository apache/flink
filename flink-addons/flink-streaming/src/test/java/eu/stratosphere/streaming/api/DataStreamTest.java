package eu.stratosphere.streaming.api;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;

import org.junit.Test;

import eu.stratosphere.api.datastream.DataStream;
import eu.stratosphere.api.datastream.FlatMapInvokable;
import eu.stratosphere.api.datastream.StreamExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.typeutils.TypeExtractor;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.MyGeneric;
import eu.stratosphere.types.TypeInformation;
import eu.stratosphere.util.Collector;

public class DataStreamTest {

	public static final class MyMap extends MapFunction<Tuple1<String>, Tuple1<String>> {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple1<String> map(Tuple1<String> value) throws Exception {
			System.out.println("in map: " + value.f0);
			return new Tuple1<String>("hahahahaha");
		}
	}

	public static final class MyFlatMap extends FlatMapFunction<Tuple1<String>, Tuple1<String>> {
		@Override
		public void flatMap(Tuple1<String> value, Collector<Tuple1<String>> out) throws Exception {

		}
	}

	@Test
	public void test() throws IOException, ClassNotFoundException {
		Tuple1<String> tup = new Tuple1<String>("asd");

		StreamExecutionEnvironment context = new StreamExecutionEnvironment();

		// DataStream<Tuple1<String>> dataStream =
		// context.setDummySource().map(new MyMap());

		DataStream<Tuple1<String>> dataStream = context.setDummySource().flatMap(new MyFlatMap());

		JobGraphBuilder jgb = context.jobGB();

		// System.out.println(jgb.components);
		for (AbstractJobVertex c : jgb.components.values()) {
			if (c instanceof JobTaskVertex) {
				Configuration config = c.getConfiguration();
				System.out.println(config.getString("componentName", "default"));
				byte[] bytes = config.getBytes("operator", null);

				ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes));

				FlatMapFunction<?,?> f = (FlatMapFunction<?,?>) in.readObject();

				System.out.println(f.getClass().getGenericSuperclass());
				TypeInformation<?> ts = (TypeInformation<?>) TypeExtractor.createTypeInfo(FlatMapFunction.class, f.getClass(), 1, null, null);
				System.out.println(ts);
				System.out.println("----------------");
			}
		}

		// context.execute(dataStream.getId());
		//
		// map(new MapFunction<Tuple1<String>, Tuple1<String>>() {
		//
		// @Override
		// public Tuple1<String> map(Tuple1<String> value) throws Exception {
		// // TODO Auto-generated method stub
		// return null;
		// }
		// });
	}
}
