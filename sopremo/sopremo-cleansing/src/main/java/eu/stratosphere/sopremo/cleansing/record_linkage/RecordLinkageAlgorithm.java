package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.List;

import eu.stratosphere.sopremo.AbstractSopremoType;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SerializableSopremoType;
import eu.stratosphere.sopremo.expressions.BooleanExpression;

// TODO: refactor
public abstract class RecordLinkageAlgorithm extends AbstractSopremoType implements IntraSourceRecordLinkageAlgorithm,
		SerializableSopremoType {
	/**
	 * 
	 */
	private static final long serialVersionUID = 8512521965472558560L;

	public abstract Operator<?> getInterSource(BooleanExpression duplicateCondition, RecordLinkageInput input1,
			RecordLinkageInput input2);

	public Operator<?> getDuplicatePairStream(BooleanExpression duplicateCondition, List<RecordLinkageInput> inputs) {
		return this.getInterSource(duplicateCondition, inputs.get(0), inputs.get(1));
	}

	@Override
	public void toString(StringBuilder builder) {
		builder.append(this.getClass().getSimpleName());
	}
}