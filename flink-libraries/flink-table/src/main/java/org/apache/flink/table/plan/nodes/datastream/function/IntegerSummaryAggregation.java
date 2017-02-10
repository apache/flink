package org.apache.flink.table.plan.nodes.datastream.function;

import org.apache.flink.api.java.summarize.aggregation.Aggregator;
import org.apache.flink.api.java.summarize.aggregation.IntegerSummaryAggregator;
import org.apache.flink.api.java.summarize.aggregation.NumericSummaryAggregator;
import org.apache.flink.api.java.summarize.aggregation.IntegerSummaryAggregator.MaxIntegerAggregator;
import org.apache.flink.api.java.summarize.aggregation.IntegerSummaryAggregator.MinIntegerAggregator;
import org.apache.flink.api.java.summarize.aggregation.IntegerSummaryAggregator.SumIntegerAggregator;

public class IntegerSummaryAggregation extends NumericSummaryAggregator<Integer> {

	private static final long serialVersionUID = 6630407882798978778L;

	public class MinIntegerAggregator implements Aggregator<Integer, Integer> {

		private static final long serialVersionUID = -3576239746558269747L;
		private int min = Integer.MAX_VALUE;

		@Override
		public void aggregate(Integer value) {
			min = Math.min(min, value);
		}

		@Override
		public void combine(Aggregator<Integer, Integer> other) {
			min = Math.min(min, ((MinIntegerAggregator) other).min);
		}

		@Override
		public Integer result() {
			return min;
		}
	}

	public class MaxIntegerAggregator implements Aggregator<Integer, Integer> {

		private static final long serialVersionUID = -8450072340111715884L;
		private int max = Integer.MIN_VALUE;

		@Override
		public void aggregate(Integer value) {
			max = Math.max(max, value);
		}

		@Override
		public void combine(Aggregator<Integer, Integer> other) {
			max = Math.max(max, ((MaxIntegerAggregator) other).max);
		}

		@Override
		public Integer result() {
			return max;
		}
	}

	public class SumIntegerAggregator implements Aggregator<Integer, Integer> {

		private static final long serialVersionUID = 7802173071935493176L;
		private int sum = 0;

		@Override
		public void aggregate(Integer value) {
			sum += value;
		}

		@Override
		public void combine(Aggregator<Integer, Integer> other) {
			sum += ((SumIntegerAggregator) other).sum;
		}

		@Override
		public Integer result() {
			return sum;
		}
	}

	@Override
	protected Aggregator<Integer, Integer> initMin() {
		return new MinIntegerAggregator();
	}

	@Override
	protected Aggregator<Integer, Integer> initMax() {
		return new MaxIntegerAggregator();
	}

	@Override
	protected Aggregator<Integer, Integer> initSum() {
		return new SumIntegerAggregator();
	}

	@Override
	protected boolean isNan(Integer number) {
		// NaN never applies here because only types like Float and Double have
		// NaN
		return false;
	}

	@Override
	protected boolean isInfinite(Integer number) {
		// Infinity never applies here because only types like Float and Double
		// have Infinity
		return false;
	}

	
}
