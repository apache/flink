package eu.stratosphere.sopremo.cleansing;

import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.stub.Stub;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator.Output;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.cleansing.RecordLinkage.Partitioning;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.JsonNodeComparator;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class Naive extends Partitioning {
	public Naive() {
	}

	@Override
	public SopremoModule asSopremoOperators(ComparativeExpression similarityCondition, List<Output> inputs,
			List<EvaluationExpression> idProjections, EvaluationExpression duplicateProjection) {
		if (inputs.size() == 1)
			return SopremoModule.valueOf(
				"Naive Record Linkage",
					new CartesianProduct(similarityCondition, duplicateProjection, inputs.get(0), idProjections.get(0),
						inputs.get(0), idProjections.get(0)));
		return SopremoModule.valueOf(
			"Naive Record Linkage",
				new CartesianProduct(similarityCondition, duplicateProjection, inputs.get(0), idProjections.get(0),
					inputs.get(1), idProjections.get(1)));
	}

	public static class CartesianProduct extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 8648299921312622401L;

		private ComparativeExpression similarityCondition;

		private EvaluationExpression duplicateProjection, idProjection1, idProjection2;

		public CartesianProduct(ComparativeExpression similarityCondition, EvaluationExpression duplicateProjection,
				JsonStream stream1, EvaluationExpression idProjection1, JsonStream stream2,
				EvaluationExpression idProjection2) {
			super(stream1, stream2);
			this.similarityCondition = similarityCondition;
			this.duplicateProjection = duplicateProjection;
			this.idProjection1 = idProjection1;
			this.idProjection2 = idProjection2;
		}

		@Override
		protected void configureContract(Configuration stubConfiguration, EvaluationContext context) {
			super.configureContract(stubConfiguration, context);
			SopremoUtil.serialize(stubConfiguration, "similarityCondition", this.similarityCondition);
			SopremoUtil.serialize(stubConfiguration, "duplicateProjection", this.duplicateProjection);
			if (this.getInput(0) == this.getInput(1)) {
				SopremoUtil.serialize(stubConfiguration, "idProjection1", this.idProjection1);
				SopremoUtil.serialize(stubConfiguration, "idProjection2", this.idProjection2);
			}
		}

		@Override
		protected Class<? extends Stub<?, ?>> getStubClass() {
			return this.getInput(0) == this.getInput(1) ? IntraSource.class : InterSource.class;
		}

		public static class InterSource
				extends
				SopremoCross<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
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
			protected void cross(JsonNode key1, JsonNode value1, JsonNode key2, JsonNode value2, JsonCollector out) {
				if (this.similarityCondition.evaluate(JsonUtil.asArray(value1, value2), this.getContext()) == BooleanNode.TRUE)
					out.collect(NullNode.getInstance(),
						this.duplicateProjection.evaluate(JsonUtil.asArray(value1, value2), this.getContext()));
			}
		}

		public static class IntraSource
				extends
				SopremoCross<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactJsonObject.Key,
				PactJsonObject> {
			private ComparativeExpression similarityCondition;

			private EvaluationExpression duplicateProjection, idProjection1, idProjection2;

			@Override
			public void configure(Configuration parameters) {
				super.configure(parameters);
				this.similarityCondition = SopremoUtil.deserialize(parameters, "similarityCondition",
					ComparativeExpression.class);
				this.duplicateProjection = SopremoUtil.deserialize(parameters, "duplicateProjection",
					EvaluationExpression.class);
				this.idProjection1 = SopremoUtil.deserialize(parameters, "idProjection1",
					EvaluationExpression.class);
				this.idProjection2 = SopremoUtil.deserialize(parameters, "idProjection2",
					EvaluationExpression.class);
			}

			@Override
			protected void cross(JsonNode key1, JsonNode value1, JsonNode key2, JsonNode value2, JsonCollector out) {
				// if( id(value1) < id(value2) && similarityCondition )
				if (JsonNodeComparator.INSTANCE.compare(this.idProjection1.evaluate(value1, this.getContext()),
					this.idProjection2.evaluate(value2, this.getContext())) < 0
					&& this.similarityCondition.evaluate(JsonUtil.asArray(value1, value2), this.getContext()) == BooleanNode.TRUE)
					out.collect(NullNode.getInstance(),
						this.duplicateProjection.evaluate(JsonUtil.asArray(value1, value2), this.getContext()));
			}
		}
	}

}