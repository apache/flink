package eu.stratosphere.sopremo.cleansing.record_linkage;

import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.BooleanExpression;

public interface IntraSourceRecordLinkageAlgorithm {
	public abstract Operator<?> getIntraSource(BooleanExpression duplicateCondition, RecordLinkageInput input);
}
