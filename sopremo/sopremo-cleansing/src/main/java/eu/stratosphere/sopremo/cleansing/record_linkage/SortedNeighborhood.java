package eu.stratosphere.sopremo.cleansing.record_linkage;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;

public class SortedNeighborhood extends MultiPassPartitioning {

	public SortedNeighborhood(final EvaluationExpression partitionKey) {
		super(partitionKey);
	}

	public SortedNeighborhood(final EvaluationExpression leftPartitionKey, final EvaluationExpression rightPartitionKey) {
		super(leftPartitionKey, rightPartitionKey);
	}

	@Override
	protected Operator<?> createSinglePassInterSource(EvaluationExpression[] partitionKeys,
			EvaluationExpression duplicateCondition, RecordLinkageInput input1, RecordLinkageInput input2) {
		return new SinglePassInterSource(partitionKeys, duplicateCondition).withInputs(input1, input2);
	}

	@Override
	protected Operator<?> createSinglePassIntraSource(EvaluationExpression partitionKey,
			EvaluationExpression duplicateCondition, RecordLinkageInput input) {
		return new SinglePassIntraSource(partitionKey, duplicateCondition).withInputs(input);
	}

	@InputCardinality(min = 2, max = 2)
	public static class SinglePassInterSource extends CompositeOperator<SinglePassInterSource> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1855028898709562743L;

		private final EvaluationExpression duplicateCondition;

		private final EvaluationExpression[] partitionKeys;

		public SinglePassInterSource(final EvaluationExpression[] partitionKeys,
				final EvaluationExpression duplicateCondition) {
			this.duplicateCondition = duplicateCondition;
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

		public EvaluationExpression getDuplicateCondition() {
			return this.duplicateCondition;
		}
	}

	@InputCardinality(min = 1, max = 1)
	public static class SinglePassIntraSource extends CompositeOperator<SinglePassIntraSource> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1107448583115047415L;

		private final EvaluationExpression duplicateCondition;

		private final EvaluationExpression partitionKey;

		public SinglePassIntraSource(final EvaluationExpression partitionKey,
				final EvaluationExpression duplicateCondition) {
			this.duplicateCondition = duplicateCondition;
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

		public EvaluationExpression getDuplicateCondition() {
			return this.duplicateCondition;
		}
	}
}