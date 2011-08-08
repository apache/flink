package eu.stratosphere.sopremo.cleansing.record_linkage;

import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
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
	protected Operator createSinglePassInterSource(final EvaluationExpression[] partitionKeys,
			final ComparativeExpression similarityCondition, final Output input1, final Output input2,
			final List<EvaluationExpression> idProjections, final EvaluationExpression duplicateProjection) {
		return new SinglePassInterSource(partitionKeys, similarityCondition, duplicateProjection, input1, input2);
	}

	@Override
	protected Operator createSinglePassIntraSource(final EvaluationExpression partitionKey,
			final ComparativeExpression similarityCondition, final Output input,
			final List<EvaluationExpression> idProjections, final EvaluationExpression duplicateProjection) {
		return new SinglePassIntraSource(partitionKey, similarityCondition, duplicateProjection, idProjections.get(0),
			input);
	}

	public static class InterSourceComparison extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2038167367269740924L;

		private final ComparativeExpression similarityCondition;

		private final EvaluationExpression duplicateProjection;

		public InterSourceComparison(final ComparativeExpression similarityCondition,
				final EvaluationExpression duplicateProjection, final JsonStream input1, final JsonStream input2) {
			super(input1, input2);
			this.similarityCondition = similarityCondition;
			this.duplicateProjection = duplicateProjection;
		}

		public EvaluationExpression getDuplicateProjection() {
			return this.duplicateProjection;
		}

		public ComparativeExpression getSimilarityCondition() {
			return this.similarityCondition;
		}

		public static class Implementation extends
				SopremoMatch<Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
			private ComparativeExpression similarityCondition;

			private EvaluationExpression duplicateProjection;

			@Override
			protected void match(final JsonNode key, final JsonNode value1, final JsonNode value2,
					final JsonCollector out) {
				final CompactArrayNode pair = JsonUtil.asArray(value1, value2);
				if (this.similarityCondition.evaluate(pair, this.getContext()) == BooleanNode.TRUE)
					out.collect(key, this.duplicateProjection.evaluate(pair, this.getContext()));
			}
		}
	}

	public static class IntraSourceComparison extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7987102025006298382L;

		@SuppressWarnings("unused")
		private final ComparativeExpression similarityCondition;

		@SuppressWarnings("unused")
		private final EvaluationExpression duplicateProjection, idProjection;

		public IntraSourceComparison(final ComparativeExpression similarityCondition,
				final EvaluationExpression duplicateProjection, final EvaluationExpression idProjection,
				final JsonStream stream) {
			super(stream, stream);
			this.similarityCondition = similarityCondition;
			this.duplicateProjection = duplicateProjection;
			this.idProjection = idProjection;
		}

		public static class Implementation extends
				SopremoMatch<Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
			private ComparativeExpression similarityCondition;

			private EvaluationExpression duplicateProjection, idProjection;

			@Override
			protected void match(final JsonNode key, final JsonNode value1, final JsonNode value2,
					final JsonCollector out) {
				if (JsonNodeComparator.INSTANCE.compare(this.idProjection.evaluate(value1, this.getContext()),
					this.idProjection.evaluate(value2, this.getContext())) < 0
					&& this.similarityCondition.evaluate(JsonUtil.asArray(value1, value2), this.getContext()) == BooleanNode.TRUE)
					out.collect(NullNode.getInstance(),
						this.duplicateProjection.evaluate(JsonUtil.asArray(value1, value2), this.getContext()));
			}
		}
	}

	public static class SinglePassInterSource extends CompositeOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8651334754653877176L;

		private final ComparativeExpression similarityCondition;

		private final EvaluationExpression[] partitionKeys;

		private final EvaluationExpression duplicateProjection;

		public SinglePassInterSource(final EvaluationExpression[] partitionKeys,
				final ComparativeExpression similarityCondition, final EvaluationExpression duplicateProjection,
				final JsonStream stream1, final JsonStream stream2) {
			super(stream1, stream2);
			this.similarityCondition = similarityCondition;
			this.partitionKeys = partitionKeys;
			this.duplicateProjection = duplicateProjection;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			final Projection[] keyExtractors = new Projection[2];
			for (int index = 0; index < 2; index++)
				keyExtractors[index] = new Projection(this.partitionKeys[index],
					EvaluationExpression.SAME_VALUE, this.getInput(index));

			return SopremoModule.valueOf(this.getName(), new InterSourceComparison(this.similarityCondition,
				this.duplicateProjection, keyExtractors[0], keyExtractors[1]));
		}

	}

	public static class SinglePassIntraSource extends CompositeOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7681741106450777330L;

		private final ComparativeExpression similarityCondition;

		private final EvaluationExpression partitionKey, duplicateProjection, idProjection;

		public SinglePassIntraSource(final EvaluationExpression partitionKey,
				final ComparativeExpression similarityCondition,
				final EvaluationExpression duplicateProjection, final EvaluationExpression idProjection,
				final JsonStream stream) {
			super(stream);
			this.similarityCondition = similarityCondition;
			this.partitionKey = partitionKey;
			this.duplicateProjection = duplicateProjection;
			this.idProjection = idProjection;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			final Projection keyExtractor = new Projection(this.partitionKey, EvaluationExpression.SAME_VALUE,
				this.getInput(0));

			return SopremoModule.valueOf(this.getName(), new IntraSourceComparison(this.similarityCondition,
				this.duplicateProjection, this.idProjection, keyExtractor));
		}
	}
}