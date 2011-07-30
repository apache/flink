package eu.stratosphere.sopremo.cleansing.conflict_resolution;

import eu.stratosphere.sopremo.aggregation.MaterializingAggregationFunction;

public class MergeResolution extends MaterializingAggregationFunction {
	public MergeResolution() {
		super("merge");
	}
}
