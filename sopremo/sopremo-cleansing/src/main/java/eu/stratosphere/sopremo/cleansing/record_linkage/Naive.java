package eu.stratosphere.sopremo.cleansing.record_linkage;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCross;

public class Naive extends RecordLinkageAlgorithm {
	@Override
	public Operator<?> getInterSource(ComparativeExpression similarityCondition, RecordLinkageInput input1,
			RecordLinkageInput input2) {
		return new InterSource(similarityCondition, input1, input2);
	}

	@Override
	public Operator<?> getIntraSource(ComparativeExpression similarityCondition, RecordLinkageInput input) {
		return new IntraSource(similarityCondition, input);
	}

	@InputCardinality(min = 2, max = 2)
	public static class InterSource extends ElementaryOperator<InterSource> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 8648299921312622401L;

		@SuppressWarnings("unused")
		private final ComparativeExpression similarityCondition;

		@SuppressWarnings("unused")
		private final EvaluationExpression resultProjection1, resultProjection2;

		public InterSource(final ComparativeExpression similarityCondition, RecordLinkageInput stream1,
				RecordLinkageInput stream2) {
			this.setInputs(stream1, stream2);
			this.similarityCondition = similarityCondition;
			this.resultProjection1 = stream1.getResultProjection();
			this.resultProjection2 = stream2.getResultProjection();
		}

		public static class Implementation
				extends
				SopremoCross<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			private ComparativeExpression similarityCondition;

			private EvaluationExpression resultProjection1, resultProjection2;

			@Override
			protected void cross(final JsonNode key1, final JsonNode value1, final JsonNode key2,
					final JsonNode value2, final JsonCollector out) {
				if (this.similarityCondition.evaluate(JsonUtil.asArray(value1, value2), this.getContext()) == BooleanNode.TRUE)
					out.collect(NullNode.getInstance(),
						JsonUtil.asArray(this.resultProjection1.evaluate(value1, this.getContext()),
							this.resultProjection2.evaluate(value2, this.getContext())));
			}
		}
	}

	@InputCardinality(min = 2, max = 2)
	public static class IntraSource extends ElementaryOperator<IntraSource> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 8648299921312622401L;

		@SuppressWarnings("unused")
		private final ComparativeExpression similarityCondition;

		@SuppressWarnings("unused")
		private final EvaluationExpression resultProjection, idProjection;

		public IntraSource(final ComparativeExpression similarityCondition, RecordLinkageInput stream) {
			this.setInputs(stream, stream);
			this.similarityCondition = similarityCondition;
			this.resultProjection = stream.getResultProjection();
			this.idProjection = stream.getIdProjection();
		}

		public static class Implementation
				extends
				SopremoCross<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactJsonObject.Key,
				PactJsonObject> {
			private ComparativeExpression similarityCondition;

			private EvaluationExpression resultProjection, idProjection;

			@Override
			protected void cross(final JsonNode key1, final JsonNode value1, final JsonNode key2,
					final JsonNode value2, final JsonCollector out) {
				// if( id(value1) < id(value2) && similarityCondition )
				if (JsonNodeComparator.INSTANCE.compare(this.idProjection.evaluate(value1, this.getContext()),
					this.idProjection.evaluate(value2, this.getContext())) < 0
					&& this.similarityCondition.evaluate(JsonUtil.asArray(value1, value2), this.getContext()) == BooleanNode.TRUE)
					out.collect(NullNode.getInstance(),
						JsonUtil.asArray(this.resultProjection.evaluate(value1, this.getContext()),
							this.resultProjection.evaluate(value2, this.getContext())));
			}
		}
	}

}