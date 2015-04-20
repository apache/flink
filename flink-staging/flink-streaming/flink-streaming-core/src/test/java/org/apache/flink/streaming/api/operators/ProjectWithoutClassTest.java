package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ProjectWithoutClassTest {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.generateSequence(1, 100).map(new MapFunction<Long, Tuple3<Long, Character, Double>>() {

			@Override
			public Tuple3<Long, Character, Double> map(Long value) throws Exception {
				return new Tuple3<Long, Character, Double>(value, 'c', (double) value);
			}
		}).project(0,2).print();

		env.execute("ProjectWithoutClassTest");
	}
}
