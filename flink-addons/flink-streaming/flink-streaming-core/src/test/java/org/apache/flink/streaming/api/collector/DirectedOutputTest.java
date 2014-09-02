package org.apache.flink.streaming.api.collector;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.junit.Test;

public class DirectedOutputTest {

	private static final String TEN = "ten";
	private static final String ODD = "odd";
	private static final String ALL = "all";
	private static final String EVEN_AND_ODD = "evenAndOdd";
	private static final String ODD_AND_TEN = "oddAndTen";
	private static final String EVEN = "even";

	static final class MyMap implements MapFunction<Long, Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public Long map(Long value) throws Exception {
			return value;
		}
	}

	static final class MyOutputSelector extends OutputSelector<Long> {
		private static final long serialVersionUID = 1L;

		@Override
		public void select(Long value, Collection<String> outputs) {
			if (value % 2 == 0) {
				outputs.add(EVEN);
			} else {
				outputs.add(ODD);
			}
			
			if (value == 10L) {
				outputs.add(TEN);
			}
		}
	}
	
	static final class ListSink implements SinkFunction<Long> {
		private static final long serialVersionUID = 1L;

		private String name;
		private transient List<Long> list;

		public ListSink(String name) {
			this.name = name;
		}

		@Override
		public void invoke(Long value) {
			list.add(value);
		}

		private void readObject(java.io.ObjectInputStream in) throws IOException,
				ClassNotFoundException {
			in.defaultReadObject();
			outputs.put(name, new ArrayList<Long>());
			this.list = outputs.get(name);
		}
	}

	private static Map<String, List<Long>> outputs = new HashMap<String, List<Long>>();
	
	@Test
	public void outputSelectorTest() throws Exception {

		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

		SplitDataStream<Long> source = env.generateSequence(1, 10).split(new MyOutputSelector());
		source.select(EVEN).addSink(new ListSink(EVEN));
		source.select(ODD, TEN).addSink(new ListSink(ODD_AND_TEN));
		source.select(EVEN, ODD).addSink(new ListSink(EVEN_AND_ODD));
		source.selectAll().addSink(new ListSink(ALL));
		
		env.executeTest(128);
		assertEquals(Arrays.asList(2L, 4L, 6L, 8L, 10L), outputs.get(EVEN));
		assertEquals(Arrays.asList(1L, 3L, 5L, 7L, 9L, 10L), outputs.get(ODD_AND_TEN));
		assertEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), outputs.get(EVEN_AND_ODD));
		assertEquals(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L), outputs.get(ALL));
	}
}
