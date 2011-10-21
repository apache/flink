package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.List;

import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.BooleanExpression;

// TODO: refactor
public abstract class RecordLinkageAlgorithm implements IntraSourceRecordLinkageAlgorithm {
	public abstract Operator<?> getInterSource(BooleanExpression duplicateCondition, RecordLinkageInput input1,
			RecordLinkageInput input2);

	public Operator<?> getDuplicatePairStream(BooleanExpression duplicateCondition, List<RecordLinkageInput> inputs) {
		return this.getInterSource(duplicateCondition, inputs.get(0), inputs.get(1));
	}
}