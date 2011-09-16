package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.List;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class SortedNeighborhood extends MultiPassPartitioning {

	public SortedNeighborhood(final EvaluationExpression partitionKey) {
		super(partitionKey);
	}

	public SortedNeighborhood(final EvaluationExpression leftPartitionKey, final EvaluationExpression rightPartitionKey) {
		super(leftPartitionKey, rightPartitionKey);
	}

	@Override
	protected Operator createSinglePassInterSource(EvaluationExpression[] partitionKeys,
			ComparativeExpression similarityCondition, RecordLinkageInput input1, RecordLinkageInput input2) {
		return new SinglePassInterSource(partitionKeys, similarityCondition).withInputs(input1, input2);
	}

	@Override
	protected Operator createSinglePassIntraSource(EvaluationExpression partitionKey,
			ComparativeExpression similarityCondition, RecordLinkageInput input) {
		return new SinglePassIntraSource(partitionKey, similarityCondition).withInputs(input);
	}

	@InputCardinality(min = 2, max = 2)
	public static class SinglePassInterSource extends CompositeOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1855028898709562743L;

		private final ComparativeExpression similarityCondition;

		private final EvaluationExpression[] partitionKeys;

		public SinglePassInterSource(final EvaluationExpression[] partitionKeys,
				final ComparativeExpression similarityCondition) {
			this.similarityCondition = similarityCondition;
			this.partitionKeys = partitionKeys;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			// final Projection[] keyExtractors = new Projection[2];
			// if (this.getInput(0) == this.getInput(1))
			// keyExtractors[0] = keyExtractors[1] = new Projection(this.partitionKeys[0],
			// EvaluationExpression.SAME_VALUE,
			// this.getInput(0));
			// else
			// for (int index = 0; index < 2; index++)
			// keyExtractors[index] = new Projection(this.partitionKeys[index], EvaluationExpression.SAME_VALUE,
			// this.getInput(index));

			throw new UnsupportedOperationException("SNM not yet supported");

			// return SopremoModule.valueOf(getName());
		}

		public EvaluationExpression[] getPartitionKeys() {
			return this.partitionKeys;
		}

		public ComparativeExpression getSimilarityCondition() {
			return this.similarityCondition;
		}
	}

	@InputCardinality(min = 1, max = 1)
	public static class SinglePassIntraSource extends CompositeOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1107448583115047415L;

		private final ComparativeExpression similarityCondition;

		private final EvaluationExpression partitionKey;

		public SinglePassIntraSource(final EvaluationExpression partitionKey,
				final ComparativeExpression similarityCondition) {
			this.similarityCondition = similarityCondition;
			this.partitionKey = partitionKey;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			// final Projection keyExtractor = new Projection(this.partitionKey, EvaluationExpression.SAME_VALUE,
			// this.getInput(0));

			throw new UnsupportedOperationException("SNM not yet supported");

			// return SopremoModule.valueOf(getName());
		}

		public EvaluationExpression getPartitionKey() {
			return this.partitionKey;
		}

		public ComparativeExpression getSimilarityCondition() {
			return this.similarityCondition;
		}
	}
}