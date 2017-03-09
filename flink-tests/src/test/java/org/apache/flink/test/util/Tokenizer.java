package org.apache.flink.test.util;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

	@Override
	public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
		// normalize and split the line
		String[] tokens = value.toLowerCase().split("\\W+");

		// emit the pairs
		for (String token : tokens) {
			if (token.length() > 0) {
				out.collect(new Tuple2<String, Integer>(token, 1));
			}
		}
	}
}
