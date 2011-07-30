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
	public DisjunctPartitioning(EvaluationExpression leftPartitionKey, EvaluationExpression rightPartitionKey) {
		super(leftPartitionKey, rightPartitionKey);
	}

	public DisjunctPartitioning(EvaluationExpression partitionKey) {
		super(partitionKey);
	}

	public DisjunctPartitioning(EvaluationExpression[] leftPartitionKeys, EvaluationExpression[] rightPartitionKeys) {
		super(leftPartitionKeys, rightPartitionKeys);
	}

	@Override
	protected Operator createSinglePassInterSource(EvaluationExpression[] partitionKeys,
			ComparativeExpression similarityCondition, Output input1, Output input2,
			List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection) {
		return new SinglePassInterSource(partitionKeys, similarityCondition, duplicateProjection, input1, input2);
	}

	@Override
	protected Operator createSinglePassIntraSource(EvaluationExpression partitionKey,
			ComparativeExpression similarityCondition, Output input,
			List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection) {
		return new SinglePassIntraSource(partitionKey, similarityCondition, duplicateProjection, idProjections.get(0),
			input);
	}

	public static class SinglePassIntraSource extends CompositeOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7681741106450777330L;

		private ComparativeExpression similarityCondition;

		private EvaluationExpression partitionKey, duplicateProjection, idProjection;

		public SinglePassIntraSource(EvaluationExpression partitionKey, ComparativeExpression similarityCondition,
				EvaluationExpression duplicateProjection, EvaluationExpression idProjection, JsonStream stream) {
			super(stream);
			this.similarityCondition = similarityCondition;
			this.partitionKey = partitionKey;
			this.duplicateProjection = duplicateProjection;
			this.idProjection = idProjection;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			Projection keyExtractor = new Projection(this.partitionKey, EvaluationExpression.SAME_VALUE,
				this.getInput(0));

			return SopremoModule.valueOf(this.getName(), new IntraSourceComparison(this.similarityCondition,
				this.duplicateProjection, this.idProjection, keyExtractor));
		}
	}

	public static class IntraSourceComparison extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7987102025006298382L;

		@SuppressWarnings("unused")
		private ComparativeExpression similarityCondition;

		@SuppressWarnings("unused")
		private EvaluationExpression duplicateProjection, idProjection;

		public IntraSourceComparison(ComparativeExpression similarityCondition,
				EvaluationExpression duplicateProjection, EvaluationExpression idProjection,
				JsonStream stream) {
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
			protected void match(JsonNode key, JsonNode value1, JsonNode value2, JsonCollector out) {
				if (JsonNodeComparator.INSTANCE.compare(this.idProjection.evaluate(value1, this.getContext()),
					this.idProjection.evaluate(value2, this.getContext())) < 0
					&& this.similarityCondition.evaluate(JsonUtil.asArray(value1, value2), this.getContext()) == BooleanNode.TRUE)
					out.collect(NullNode.getInstance(),
						this.duplicateProjection.evaluate(JsonUtil.asArray(value1, value2), this.getContext()));
			}
		}
	}

	public static class InterSourceComparison extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2038167367269740924L;

		private ComparativeExpression similarityCondition;

		private EvaluationExpression duplicateProjection;

		public InterSourceComparison(ComparativeExpression similarityCondition,
				EvaluationExpression duplicateProjection, JsonStream input1, JsonStream input2) {
			super(input1, input2);
			this.similarityCondition = similarityCondition;
			this.duplicateProjection = duplicateProjection;
		}

		public static class Implementation extends
				SopremoMatch<Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
			private ComparativeExpression similarityCondition;

			private EvaluationExpression duplicateProjection;

			@Override
			protected void match(JsonNode key, JsonNode value1, JsonNode value2, JsonCollector out) {
				CompactArrayNode pair = JsonUtil.asArray(value1, value2);
				if (this.similarityCondition.evaluate(pair, this.getContext()) == BooleanNode.TRUE)
					out.collect(key, this.duplicateProjection.evaluate(pair, this.getContext()));
			}
		}
	}

	public static class SinglePassInterSource extends CompositeOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -8651334754653877176L;

		private ComparativeExpression similarityCondition;

		private EvaluationExpression[] partitionKeys;

		private EvaluationExpression duplicateProjection;

		public SinglePassInterSource(EvaluationExpression[] partitionKeys,
				ComparativeExpression similarityCondition, EvaluationExpression duplicateProjection,
				JsonStream stream1, JsonStream stream2) {
			super(stream1, stream2);
			this.similarityCondition = similarityCondition;
			this.partitionKeys = partitionKeys;
			this.duplicateProjection = duplicateProjection;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			Projection[] keyExtractors = new Projection[2];
			for (int index = 0; index < 2; index++)
				keyExtractors[index] = new Projection(this.partitionKeys[index],
						EvaluationExpression.SAME_VALUE, this.getInput(index));

			return SopremoModule.valueOf(this.getName(), new InterSourceComparison(this.similarityCondition,
				this.duplicateProjection, keyExtractors[0], keyExtractors[1]));
		}

	}
}