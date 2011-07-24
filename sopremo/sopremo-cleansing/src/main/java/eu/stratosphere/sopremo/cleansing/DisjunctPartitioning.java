package eu.stratosphere.sopremo.cleansing;

import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.sopremo.CompactArrayNode;
import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.base.Projection;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.PactJsonObject.Key;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoReduce;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class DisjunctPartitioning extends MultiPassPartitioning {
	public DisjunctPartitioning(EvaluationExpression leftPartitionKey, EvaluationExpression rightPartitionKey) {
		super(leftPartitionKey, rightPartitionKey);
	}

	public DisjunctPartitioning(EvaluationExpression partitionKey) {
		super(partitionKey);
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
		return new SinglePassIntraSource(partitionKey, similarityCondition, duplicateProjection, input);
	}

	public static class SinglePassIntraSource extends CompositeOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7681741106450777330L;

		private ComparativeExpression similarityCondition;

		private EvaluationExpression partitionKey, duplicateProjection;

		public SinglePassIntraSource(EvaluationExpression partitionKey, ComparativeExpression similarityCondition,
				EvaluationExpression duplicateProjection, JsonStream stream) {
			super(stream);
			this.similarityCondition = similarityCondition;
			this.partitionKey = partitionKey;
			this.duplicateProjection = duplicateProjection;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			Projection keyExtractor = new Projection(this.partitionKey, EvaluationExpression.SAME_VALUE,
				this.getInput(0));

			return SopremoModule.valueOf(this.getName(), new IntraSourceComparison(this.similarityCondition,
				this.duplicateProjection,
				keyExtractor));
		}
	}

	public static class IntraSourceComparison extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7987102025006298382L;

		private ComparativeExpression similarityCondition;

		private EvaluationExpression duplicateProjection;

		public IntraSourceComparison(ComparativeExpression similarityCondition,
				EvaluationExpression duplicateProjection, JsonStream input) {
			super(input);
			this.similarityCondition = similarityCondition;
			this.duplicateProjection = duplicateProjection;
		}

		@Override
		protected void configureContract(Configuration stubConfiguration, EvaluationContext context) {
			super.configureContract(stubConfiguration, context);
			SopremoUtil.serialize(stubConfiguration, "similarityCondition", this.similarityCondition);
			SopremoUtil.serialize(stubConfiguration, "duplicateProjection", this.duplicateProjection);
		}

		public static class Implementation extends SopremoReduce<Key, PactJsonObject, Key, PactJsonObject> {
			private ComparativeExpression similarityCondition;

			private EvaluationExpression duplicateProjection;

			@Override
			public void configure(Configuration parameters) {
				super.configure(parameters);
				this.similarityCondition = SopremoUtil.deserialize(parameters, "similarityCondition",
					ComparativeExpression.class);
				this.duplicateProjection = SopremoUtil.deserialize(parameters, "duplicateProjection",
					EvaluationExpression.class);
			}

			@Override
			protected void reduce(JsonNode key, StreamArrayNode values, JsonCollector out) {
				values = values.ensureResettable();

				int size = values.size();
				for (int index = 0; index < size; index++) {
					JsonNode value1 = values.get(index);
					for (int index2 = index + 1; index2 < size; index2++) {
						JsonNode value2 = values.get(index2);
						CompactArrayNode pair = JsonUtil.asArray(value1, value2);
						if (this.similarityCondition.evaluate(pair, this.getContext()) == BooleanNode.TRUE)
							out.collect(key, this.duplicateProjection.evaluate(pair, this.getContext()));
					}
				}
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

		@Override
		protected void configureContract(Configuration stubConfiguration, EvaluationContext context) {
			super.configureContract(stubConfiguration, context);
			SopremoUtil.serialize(stubConfiguration, "similarityCondition", this.similarityCondition);
			SopremoUtil.serialize(stubConfiguration, "duplicateProjection", this.duplicateProjection);
		}

		public static class Implementation extends
				SopremoCoGroup<Key, PactJsonObject, PactJsonObject, Key, PactJsonObject> {
			private ComparativeExpression similarityCondition;

			private EvaluationExpression duplicateProjection;

			@Override
			public void configure(Configuration parameters) {
				super.configure(parameters);
				this.similarityCondition = SopremoUtil.deserialize(parameters, "similarityCondition",
					ComparativeExpression.class);
				this.duplicateProjection = SopremoUtil.deserialize(parameters, "duplicateProjection",
					EvaluationExpression.class);
			}

			@Override
			protected void coGroup(JsonNode key, StreamArrayNode values1, StreamArrayNode values2, JsonCollector out) {
				values2 = values2.ensureResettable();

				for (JsonNode value1 : values1)
					for (JsonNode value2 : values2) {
						CompactArrayNode pair = JsonUtil.asArray(value1, value2);
						if (this.similarityCondition.evaluate(pair, this.getContext()) == BooleanNode.TRUE)
							out.collect(key, this.duplicateProjection.evaluate(pair, this.getContext()));
					}
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