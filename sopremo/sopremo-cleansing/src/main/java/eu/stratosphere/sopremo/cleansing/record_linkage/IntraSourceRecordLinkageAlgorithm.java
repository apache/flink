package eu.stratosphere.sopremo.cleansing.record_linkage;

import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;

public interface IntraSourceRecordLinkageAlgorithm {
	public abstract Operator<?> getIntraSource(ComparativeExpression similarityCondition, RecordLinkageInput input);
}
