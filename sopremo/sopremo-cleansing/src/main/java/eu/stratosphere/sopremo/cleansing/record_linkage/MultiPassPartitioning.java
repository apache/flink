package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Union;
import eu.stratosphere.sopremo.cleansing.record_linkage.RecordLinkage.Partitioning;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public abstract class MultiPassPartitioning extends Partitioning {
	private final List<EvaluationExpression[]> passPartitionKeys = new ArrayList<EvaluationExpression[]>();

	public MultiPassPartitioning(final EvaluationExpression partitionKey) {
		this.passPartitionKeys.add(new EvaluationExpression[] { partitionKey, partitionKey });
	}

	public MultiPassPartitioning(final EvaluationExpression leftPartitionKey,
			final EvaluationExpression rightPartitionKey) {
		this.passPartitionKeys.add(new EvaluationExpression[] { leftPartitionKey, rightPartitionKey });
	}

	public MultiPassPartitioning(final EvaluationExpression[] leftPartitionKeys,
			final EvaluationExpression[] rightPartitionKeys) {
		if (leftPartitionKeys.length != rightPartitionKeys.length)
			throw new IllegalArgumentException();
		for (int index = 0; index < leftPartitionKeys.length; index++)
			this.passPartitionKeys
				.add(new EvaluationExpression[] { leftPartitionKeys[index], rightPartitionKeys[index] });
	}

	public MultiPassPartitioning addPass(final EvaluationExpression partitionKey) {
		this.passPartitionKeys.add(new EvaluationExpression[] { partitionKey, partitionKey });
		return this;
	}

	public MultiPassPartitioning addPass(final EvaluationExpression leftPartitionKey,
			final EvaluationExpression rightPartitionKey) {
		this.passPartitionKeys.add(new EvaluationExpression[] { leftPartitionKey, rightPartitionKey });
		return this;
	}

	@Override
	public SopremoModule asSopremoOperators(final ComparativeExpression similarityCondition, final List<Output> inputs,
			final List<EvaluationExpression> idProjections, final EvaluationExpression duplicateProjection) {
		final List<Operator> passes = new ArrayList<Operator>();

		if (inputs.size() == 1)
			for (int index = 0; index < this.passPartitionKeys.size(); index++)
				passes.add(this.createSinglePassIntraSource(this.passPartitionKeys.get(index)[0], similarityCondition,
					inputs.get(0), idProjections, duplicateProjection));
		else
			for (int index = 0; index < this.passPartitionKeys.size(); index++)
				passes.add(this.createSinglePassInterSource(this.passPartitionKeys.get(index), similarityCondition,
					inputs.get(0), inputs.get(1), idProjections, duplicateProjection));

		return SopremoModule.valueOf(this.toString(), new Union(passes));
	}

	protected abstract Operator createSinglePassInterSource(EvaluationExpression[] partitionKeys,
			ComparativeExpression similarityCondition, Operator.Output input1, Operator.Output input2,
			List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection);

	protected abstract Operator createSinglePassIntraSource(EvaluationExpression partitionKey,
			ComparativeExpression similarityCondition, Operator.Output input,
			List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection);

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder(this.getClass().getSimpleName()).append(" on ");
		for (int index = 0; index < this.passPartitionKeys.size(); index++) {
			if (index > 0)
				builder.append(", ");
			builder.append(Arrays.asList(this.passPartitionKeys.get(index)));
		}
		return builder.toString();
	}
}