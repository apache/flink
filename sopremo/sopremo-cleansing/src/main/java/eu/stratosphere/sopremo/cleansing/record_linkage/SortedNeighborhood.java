package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.List;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class SortedNeighborhood extends MultiPassPartitioning {

	public SortedNeighborhood(EvaluationExpression leftPartitionKey, EvaluationExpression rightPartitionKey) {
		super(leftPartitionKey, rightPartitionKey);
	}

	public SortedNeighborhood(EvaluationExpression partitionKey) {
		super(partitionKey);
	}

	@Override
	protected Operator createSinglePassInterSource(EvaluationExpression[] partitionKeys,
			ComparativeExpression similarityCondition, Output input1, Output input2,
			List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection) {
		return new SinglePassInterSource(partitionKeys, similarityCondition, input1, input2);
	}

	@Override
	protected Operator createSinglePassIntraSource(EvaluationExpression partitionKey,
			ComparativeExpression similarityCondition, Output input,
			List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection) {
		return new SinglePassIntraSource(partitionKey, similarityCondition, input);
	}

	public static class SinglePassIntraSource extends CompositeOperator {
		private ComparativeExpression similarityCondition;

		private EvaluationExpression partitionKey;

		public SinglePassIntraSource(EvaluationExpression partitionKey, ComparativeExpression similarityCondition,
				JsonStream stream) {
			super(stream);
			this.similarityCondition = similarityCondition;
			this.partitionKey = partitionKey;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			Projection keyExtractor = new Projection(partitionKey, EvaluationExpression.SAME_VALUE, getInput(0));

			throw new UnsupportedOperationException("SNM not yet supported");

			// return SopremoModule.valueOf(getName());
		}
	}

	public static class SinglePassInterSource extends CompositeOperator {
		private ComparativeExpression similarityCondition;

		private EvaluationExpression[] partitionKeys;

		public SinglePassInterSource(EvaluationExpression[] partitionKeys,
				ComparativeExpression similarityCondition,
				JsonStream stream1, JsonStream stream2) {
			super(stream1, stream2);
			this.similarityCondition = similarityCondition;
			this.partitionKeys = partitionKeys;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			Projection[] keyExtractors = new Projection[2];
			if (getInput(0) == getInput(1))
				keyExtractors[0] = keyExtractors[1] = new Projection(partitionKeys[0], EvaluationExpression.SAME_VALUE,
					getInput(0));
			else
				for (int index = 0; index < 2; index++)
					keyExtractors[index] = new Projection(partitionKeys[index], EvaluationExpression.SAME_VALUE,
						getInput(index));

			throw new UnsupportedOperationException("SNM not yet supported");

			// return SopremoModule.valueOf(getName());
		}
	}
}