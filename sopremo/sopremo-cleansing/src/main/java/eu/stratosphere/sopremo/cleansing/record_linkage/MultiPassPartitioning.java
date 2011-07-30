package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.cleansing.record_linkage.RecordLinkage.Partitioning;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class MultiPassPartitioning extends Partitioning {
	private List<EvaluationExpression[]> passPartitionKeys = new ArrayList<EvaluationExpression[]>();

	public MultiPassPartitioning(EvaluationExpression leftPartitionKey, EvaluationExpression rightPartitionKey) {
		this.passPartitionKeys.add(new EvaluationExpression[] { leftPartitionKey, rightPartitionKey });
	}

	public MultiPassPartitioning(EvaluationExpression[] leftPartitionKeys, EvaluationExpression[] rightPartitionKeys) {
		if (leftPartitionKeys.length != rightPartitionKeys.length)
			throw new IllegalArgumentException();
		for (int index = 0; index < leftPartitionKeys.length; index++)
			this.passPartitionKeys
				.add(new EvaluationExpression[] { leftPartitionKeys[index], rightPartitionKeys[index] });
	}

	public MultiPassPartitioning(EvaluationExpression partitionKey) {
		this.passPartitionKeys.add(new EvaluationExpression[] { partitionKey, partitionKey });
	}

	public MultiPassPartitioning addPass(EvaluationExpression leftPartitionKey, EvaluationExpression rightPartitionKey) {
		this.passPartitionKeys.add(new EvaluationExpression[] { leftPartitionKey, rightPartitionKey });
		return this;
	}

	public MultiPassPartitioning addPass(EvaluationExpression partitionKey) {
		this.passPartitionKeys.add(new EvaluationExpression[] { partitionKey, partitionKey });
		return this;
	}

	@Override
	public SopremoModule asSopremoOperators(ComparativeExpression similarityCondition, List<Output> inputs,
			List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection) {
		List<Operator> passes = new ArrayList<Operator>();

		if (inputs.size() == 1)
			for (int index = 0; index < passPartitionKeys.size(); index++)
				passes.add(createSinglePassIntraSource(passPartitionKeys.get(index)[0], similarityCondition,
					inputs.get(0), idProjections, duplicateProjection));
		else
			for (int index = 0; index < passPartitionKeys.size(); index++)
				passes.add(createSinglePassInterSource(passPartitionKeys.get(index), similarityCondition,
					inputs.get(0), inputs.get(1), idProjections, duplicateProjection));

		return SopremoModule.valueOf(toString(), new Union(passes));
	}

	protected abstract Operator createSinglePassInterSource(EvaluationExpression[] partitionKeys,
			ComparativeExpression similarityCondition, Operator.Output input1, Operator.Output input2,
			List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection);

	protected abstract Operator createSinglePassIntraSource(EvaluationExpression partitionKey,
			ComparativeExpression similarityCondition, Operator.Output input,
			List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection);

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder(getClass().getSimpleName()).append(" on ");
		for (int index = 0; index < this.passPartitionKeys.size(); index++) {
			if (index > 0)
				builder.append(", ");
			builder.append(Arrays.asList(this.passPartitionKeys.get(index)));
		}
		return builder.toString();
	}
}