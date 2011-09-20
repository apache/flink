package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.List;

import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;

// TODO: refactor
public abstract class RecordLinkageAlgorithm implements IntraSourceRecordLinkageAlgorithm {
	public abstract Operator<?> getInterSource(ComparativeExpression similarityCondition, RecordLinkageInput input1,
			RecordLinkageInput input2);

	public Operator<?> getDuplicatePairStream(ComparativeExpression similarityCondition, List<RecordLinkageInput> inputs) {
		return this.getInterSource(similarityCondition, inputs.get(0), inputs.get(1));
	}
}