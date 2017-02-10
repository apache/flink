package org.apache.flink.table.plan.nodes.datastream.function;

import static org.apache.flink.api.java.summarize.aggregation.CompensatedSum.ZERO;

import org.apache.flink.api.java.summarize.aggregation.Aggregator;
import org.apache.flink.api.java.summarize.aggregation.CompensatedSum;
import org.apache.flink.api.java.summarize.aggregation.DoubleSummaryAggregator;
import org.apache.flink.api.java.summarize.aggregation.NumericSummaryAggregator;
import org.apache.flink.api.java.summarize.aggregation.DoubleSummaryAggregator.MaxDoubleAggregator;
import org.apache.flink.api.java.summarize.aggregation.DoubleSummaryAggregator.MinDoubleAggregator;
import org.apache.flink.api.java.summarize.aggregation.DoubleSummaryAggregator.SumDoubleAggregator;

public class DoubleSummaryAggregation extends NumericSummaryAggregator<Double> {

	private static final long serialVersionUID = 1L;

	public class MinDoubleAggregator implements Aggregator<Double,Double> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private double min = Double.MAX_VALUE;

		@Override
		public void aggregate(Double value) {
			min = Math.min(min, value);
		}

		@Override
		public void combine(Aggregator<Double, Double> other) {
			min = Math.min(min,((MinDoubleAggregator)other).min);
		}

		@Override
		public Double result() {
			return min;
		}
	}

	public class MaxDoubleAggregator implements Aggregator<Double,Double> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private double max = Double.MIN_VALUE;

		@Override
		public void aggregate(Double value) {
			max = Math.max(max, value);
		}

		@Override
		public void combine(Aggregator<Double, Double> other) {
			max = Math.max(max, ((MaxDoubleAggregator) other).max);
		}

		@Override
		public Double result() {
			return max;
		}
	}

	public class SumDoubleAggregator implements Aggregator<Double,Double> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private CompensatedSum sum = ZERO;

		@Override
		public void aggregate(Double value) {
			sum = sum.add(value);
		}

		@Override
		public void combine(Aggregator<Double, Double> other) {
			sum = sum.add(((SumDoubleAggregator)other).sum);
		}

		@Override
		public Double result() {
			return sum.value();
		}
	}

	@Override
	protected Aggregator<Double, Double> initMin() {
		return new MinDoubleAggregator();
	}

	@Override
	protected Aggregator<Double, Double> initMax() {
		return new MaxDoubleAggregator();
	}

	@Override
	protected Aggregator<Double, Double> initSum() {
		return new SumDoubleAggregator();
	}

	@Override
	protected boolean isNan(Double number) {
		return number.isNaN();
	}

	@Override
	protected boolean isInfinite(Double number) {
		return number.isInfinite();
	}
	
}
