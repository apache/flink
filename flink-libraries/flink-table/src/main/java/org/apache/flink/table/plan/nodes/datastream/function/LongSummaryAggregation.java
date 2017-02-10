package org.apache.flink.table.plan.nodes.datastream.function;

import org.apache.flink.api.java.summarize.aggregation.Aggregator;
import org.apache.flink.api.java.summarize.aggregation.NumericSummaryAggregator;

public class LongSummaryAggregation extends NumericSummaryAggregator<Long> {

	private static final long serialVersionUID = 1L;

	// Nested classes are only "public static" for Kryo serialization, otherwise they'd be private

	public class MinLongAggregator implements Aggregator<Long,Long> {

		private static final long serialVersionUID = 1L;
		private long min = Long.MAX_VALUE;

		@Override
		public void aggregate(Long value) {
			min = Math.min(min, value);
		}

		@Override
		public void combine(Aggregator<Long, Long> other) {
			min = Math.min(min,((MinLongAggregator)other).min);
		}

		@Override
		public Long result() {
			return min;
		}
	}

	public class MaxLongAggregator implements Aggregator<Long,Long> {

		private static final long serialVersionUID = 1L;
		private long max = Long.MIN_VALUE;

		@Override
		public void aggregate(Long value) {
			max = Math.max(max, value);
		}

		@Override
		public void combine(Aggregator<Long, Long> other) {
			max = Math.max(max, ((MaxLongAggregator) other).max);
		}

		@Override
		public Long result() {
			return max;
		}
	}

	public class SumLongAggregator implements Aggregator<Long,Long> {

		private static final long serialVersionUID = 1L;
		private long sum = 0;

		@Override
		public void aggregate(Long value) {
			sum += value;
		}

		@Override
		public void combine(Aggregator<Long, Long> other) {
			sum += ((SumLongAggregator)other).sum;
		}

		@Override
		public Long result() {
			return sum;
		}
	}

	@Override
	protected Aggregator<Long, Long> initMin() {
		return new MinLongAggregator();
	}

	@Override
	protected Aggregator<Long, Long> initMax() {
		return new MaxLongAggregator();
	}

	@Override
	protected Aggregator<Long, Long> initSum() {
		return new SumLongAggregator();
	}

	@Override
	protected boolean isNan(Long number) {
		// NaN never applies here because only types like Float and Double have NaN
		return false;
	}

	@Override
	protected boolean isInfinite(Long number) {
		// Infinity never applies here because only types like Float and Double have Infinity
		return false;
	}
}
