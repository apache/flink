package eu.stratosphere.sopremo.cleansing.record_linkage;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.jsondatamodel.ArrayNode;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
import eu.stratosphere.sopremo.jsondatamodel.JsonNode;
import eu.stratosphere.sopremo.jsondatamodel.NullNode;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMatch;

public class DisjunctPartitioning extends MultiPassPartitioning {
	public DisjunctPartitioning(final EvaluationExpression partitionKey) {
		super(partitionKey);
	}

	public DisjunctPartitioning(final EvaluationExpression leftPartitionKey,
			final EvaluationExpression rightPartitionKey) {
		super(leftPartitionKey, rightPartitionKey);
	}

	public DisjunctPartitioning(final EvaluationExpression[] leftPartitionKeys,
			final EvaluationExpression[] rightPartitionKeys) {
		super(leftPartitionKeys, rightPartitionKeys);
	}

	@Override
	protected Operator<?> createSinglePassInterSource(EvaluationExpression[] partitionKeys,
			ComparativeExpression similarityCondition, RecordLinkageInput input1, RecordLinkageInput input2) {
		return new SinglePassInterSource(partitionKeys, similarityCondition, input1, input2);
	}

	@Override
	protected Operator<?> createSinglePassIntraSource(EvaluationExpression partitionKey,
			ComparativeExpression similarityCondition, RecordLinkageInput input) {
		return new SinglePassIntraSource(partitionKey, similarityCondition, input);
	}

	@InputCardinality(min = 2, max = 2)
	public static class InterSourceComparison extends ElementaryOperator<InterSourceComparison> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2038167367269740924L;

		private final ComparativeExpression similarityCondition;

		private final EvaluationExpression resultProjection1, resultProjection2;

		public InterSourceComparison(final ComparativeExpression similarityCondition, final JsonStream input1,
				EvaluationExpression resultProjection1, final JsonStream input2, EvaluationExpression resultProjection2) {
			this.setInputs(input1, input2);
			this.similarityCondition = similarityCondition;
			this.resultProjection1 = resultProjection1;
			this.resultProjection2 = resultProjection2;
		}

		public EvaluationExpression getResultProjection1() {
			return this.resultProjection1;
		}

		public EvaluationExpression getResultProjection2() {
			return this.resultProjection2;
		}

		public ComparativeExpression getSimilarityCondition() {
			return this.similarityCondition;
		}

		public static class Implementation extends
				SopremoMatch<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> {
			private ComparativeExpression similarityCondition;

			private EvaluationExpression resultProjection1, resultProjection2;

			@Override
			protected void match(final JsonNode key, final JsonNode value1, final JsonNode value2,
					final JsonCollector out) {
				final ArrayNode pair = JsonUtil.asArray(value1, value2);
				if (this.similarityCondition.evaluate(pair, this.getContext()) == BooleanNode.TRUE)
					out.collect(NullNode.getInstance(),
						JsonUtil.asArray(this.resultProjection1.evaluate(value1, this.getContext()),
							this.resultProjection2.evaluate(value2, this.getContext())));
			}
		}
	}

	@InputCardinality(min = 2, max = 2)
	public static class IntraSourceComparison extends ElementaryOperator<IntraSourceComparison> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7987102025006298382L;

		@SuppressWarnings("unused")
		private final ComparativeExpression similarityCondition;

		@SuppressWarnings("unused")
		private final EvaluationExpression resultProjection, idProjection;

		public IntraSourceComparison(final ComparativeExpression similarityCondition,
				final EvaluationExpression resultProjection, final EvaluationExpression idProjection,
				final JsonStream stream) {
			this.setInputs(stream, stream);
			this.similarityCondition = similarityCondition;
			this.resultProjection = resultProjection;
			this.idProjection = idProjection;
		}

		public static class Implementation extends
				SopremoMatch<JsonNode, JsonNode, JsonNode, JsonNode, JsonNode> {
			private ComparativeExpression similarityCondition;

			private EvaluationExpression resultProjection, idProjection;

			@Override
			protected void match(final JsonNode key, final JsonNode value1, final JsonNode value2,
					final JsonCollector out) {
				JsonNode id1 = this.idProjection.evaluate(value1, this.getContext());
				JsonNode id2 = this.idProjection.evaluate(value2, this.getContext());
				if (id1.compareTo(id2) < 0
					&& this.similarityCondition.evaluate(JsonUtil.asArray(value1, value2), this.getContext()) == BooleanNode.TRUE)
					out.collect(NullNode.getInstance(),
						JsonUtil.asArray(this.resultProjection.evaluate(value1, this.getContext()),
							this.resultProjection.evaluate(value2, this.getContext())));
			}
		}
	}

	public static class SinglePassInterSource extends CompositeOperator<SinglePassInterSource> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8651334754653877176L;

		private final ComparativeExpression similarityCondition;

		private final EvaluationExpression[] partitionKeys;

		private final EvaluationExpression resultProjection1, resultProjection2;

		public SinglePassInterSource(final EvaluationExpression[] partitionKeys,
				final ComparativeExpression similarityCondition, final RecordLinkageInput stream1,
				final RecordLinkageInput stream2) {
			this.setInputs(stream1, stream2);
			this.similarityCondition = similarityCondition;
			this.partitionKeys = partitionKeys;
			this.resultProjection1 = stream1.getResultProjection();
			this.resultProjection2 = stream2.getResultProjection();
		}

		@Override
		public SopremoModule asElementaryOperators() {
			final Projection[] keyExtractors = new Projection[2];
			for (int index = 0; index < 2; index++)
				keyExtractors[index] = new Projection().
					withKeyTransformation(this.partitionKeys[index]).
					withInputs(this.getInput(index));

			return SopremoModule.valueOf(this.getName(), new InterSourceComparison(this.similarityCondition,
				keyExtractors[0], this.resultProjection1, keyExtractors[1], this.resultProjection2));
		}

	}

	public static class SinglePassIntraSource extends CompositeOperator<SinglePassIntraSource> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7681741106450777330L;

		private final ComparativeExpression similarityCondition;

		private final EvaluationExpression partitionKey;

		private final EvaluationExpression resultProjection, idProjection;

		public SinglePassIntraSource(final EvaluationExpression partitionKey,
				final ComparativeExpression similarityCondition, final RecordLinkageInput stream) {
			this.setInputs(stream);
			this.similarityCondition = similarityCondition;
			this.partitionKey = partitionKey;
			this.idProjection = stream.getIdProjection();
			this.resultProjection = stream.getResultProjection();
		}

		@Override
		public SopremoModule asElementaryOperators() {
			final Projection keyExtractor = new Projection().
				withKeyTransformation(this.partitionKey).
				withInputs(this.getInput(0));

			return SopremoModule.valueOf(this.getName(), new IntraSourceComparison(this.similarityCondition,
				this.resultProjection, this.idProjection, keyExtractor));
		}
	}
}