package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/***/
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
	/**
	 * The ValueState handle. The first field is the count, the second field a running sum.
	 */
	private transient ValueState<Tuple2<Long, Long>> sum;

	@Override
	public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {
		// access the state value
		Tuple2<Long, Long> currentSum = sum.value();

		// update the count
		currentSum.f0 += 1;

		// add the second field of the input value
		currentSum.f1 += input.f1;

		// update the state
		sum.update(currentSum);

		// if the count reaches 2, emit the average and clear the state
		if (currentSum.f0 >= 2) {
			out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
			sum.clear();
		}
	}

	@Override
	public void open(Configuration config) {
		ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
			new ValueStateDescriptor<>(
				"average", // the state name
				TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}),  // type information
				Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
		sum = getRuntimeContext().getState(descriptor);
	}

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStateBackend(new FsStateBackend("file://tmp/test/"));
		env.enableCheckpointing(100);
		env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
			.keyBy(value -> value.f0)
			.flatMap(new CountWindowAverage())
			.print();

		env.execute();
	}
}
