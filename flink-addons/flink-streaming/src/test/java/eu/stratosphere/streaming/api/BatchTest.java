package eu.stratosphere.streaming.api;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.util.Collector;

public class BatchTest {

	private static int count = 0;
	
	private static final class MySource extends SourceFunction<Tuple1<String>> {

		private Tuple1<String> outTuple = new Tuple1<String>();
		
		@Override
		public void invoke(Collector<Tuple1<String>> collector) throws Exception {
			for (int i = 0; i < 20; i++) {
				outTuple.f0 = "string #" + i;
				collector.collect(outTuple);
			}
		}
	}
	
	private static final class MyMap extends FlatMapFunction<Tuple1<String>, Tuple1<String>> {

		@Override
		public void flatMap(Tuple1<String> value, Collector<Tuple1<String>> out) throws Exception {
			out.collect(value);
		}
	}

	private static final class MySink extends SinkFunction<Tuple1<String>> {
		
		@Override
		public void invoke(Tuple1<String> tuple) {
			count++;
		}
	}
	
	@Test
	public void test() throws Exception {
		StreamExecutionEnvironment context = new StreamExecutionEnvironment();

		DataStream<Tuple1<String>> dataStream = context
				.addSource(new MySource())
				.flatMap(new MyMap()).batch(4)
				.flatMap(new MyMap()).batch(2)
				.flatMap(new MyMap()).batch(5)
				.flatMap(new MyMap()).batch(4)
				.addSink(new MySink());

		context.execute();
		
		assertEquals(20, count);
	}
}
