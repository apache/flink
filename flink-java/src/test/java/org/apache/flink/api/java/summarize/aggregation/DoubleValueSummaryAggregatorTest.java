package org.apache.flink.api.java.summarize.aggregation;

import org.apache.flink.api.java.summarize.NumericColumnSummary;
import org.apache.flink.types.DoubleValue;
import org.junit.Assert;

public class DoubleValueSummaryAggregatorTest extends DoubleSummaryAggregatorTest {

	/**
	 * Helper method for summarizing a list of values.
	 *
	 * This method breaks the rule of "testing only one thing" by aggregating and combining
	 * a bunch of different ways.
	 */
	protected NumericColumnSummary<Double> summarize(Double... values) {

		DoubleValue[] doubleValues = new DoubleValue[values.length];
		for(int i = 0; i < values.length; i++) {
			if (values[i] != null) {
				doubleValues[i] = new DoubleValue(values[i]);
			}
		}

		return new AggregateCombineHarness<DoubleValue,NumericColumnSummary<Double>,ValueSummaryAggregator.DoubleValueSummaryAggregator>() {

			@Override
			protected void compareResults(NumericColumnSummary<Double> result1, NumericColumnSummary<Double> result2) {
				Assert.assertEquals(result1.getMin(), result2.getMin(), 0.0);
				Assert.assertEquals(result1.getMax(), result2.getMax(), 0.0);
				Assert.assertEquals(result1.getMean(), result2.getMean(), 1e-12d);
				Assert.assertEquals(result1.getVariance(), result2.getVariance(), 1e-9d);
				Assert.assertEquals(result1.getStandardDeviation(), result2.getStandardDeviation(), 1e-12d);
			}

		}.summarize(doubleValues);
	}
}
