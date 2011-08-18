package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.List;

import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.cleansing.record_linkage.RecordLinkage.RecordLinkageInput;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class RecordLinkageAlgorithm {
	public abstract Operator getIntraSource(ComparativeExpression similarityCondition, RecordLinkageInput input);

	public abstract Operator getInterSource(ComparativeExpression similarityCondition, RecordLinkageInput input1,
			RecordLinkageInput input2);

	public Operator getDuplicatePairStream(ComparativeExpression similarityCondition, List<RecordLinkageInput> inputs) {
		if (inputs.size() == 1)
			return this.getIntraSource(similarityCondition, inputs.get(0));

		return getInterSource(similarityCondition, inputs.get(0), inputs.get(1));
	}
}