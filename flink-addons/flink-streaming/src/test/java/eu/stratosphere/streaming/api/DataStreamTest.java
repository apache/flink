package eu.stratosphere.streaming.api;

import org.junit.Test;

import eu.stratosphere.api.datastream.DataStream;
import eu.stratosphere.api.datastream.StreamExecutionEnvironment;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
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
	public void test() {
		Tuple1<String> tup = new Tuple1<String>("asd");

		StreamExecutionEnvironment context = new StreamExecutionEnvironment();

		// DataStream<Tuple1<String>> dataStream = context.setDummySource().map(new MyMap());

		DataStream<Tuple1<String>> dataStream = context.setDummySource().flatMap(new MyFlatMap());

		
		context.execute(dataStream.getId());
//				
//				map(new MapFunction<Tuple1<String>, Tuple1<String>>() {
//
//			@Override
//			public Tuple1<String> map(Tuple1<String> value) throws Exception {
//				// TODO Auto-generated method stub
//				return null;
//			}
//		});
	}
}
