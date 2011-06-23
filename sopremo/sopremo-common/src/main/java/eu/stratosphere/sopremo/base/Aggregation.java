package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.pact.common.stub.Collector;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluableExpression;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoReduce;

public class Aggregation extends MultiSourceOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1452280003631381562L;

	private final static EvaluableExpression NO_GROUPING = new ConstantExpression(NullNode.getInstance());

	private EvaluableExpression expression;

	public Aggregation(EvaluableExpression expression, JsonStream... inputs) {
		super(inputs);
		this.expression = expression;

		this.setDefaultKeyProjection(NO_GROUPING);
	}

	public Aggregation(EvaluableExpression expression, List<? extends JsonStream> inputs) {
		super(inputs);
		this.expression = expression;

		this.setDefaultKeyProjection(NO_GROUPING);
	}

	// @Override
	// protected EvaluableExpression getDefaultValueProjection(Output source) {
	// if (getInputs().size() == 1)
	// return EvaluableExpression.IDENTITY;
	// EvaluableExpression[] elements = new EvaluableExpression[getInputs().size()];
	// for (int index = 0; index < elements.length; index++)
	// elements[index] = EvaluableExpression.NULL;
	// elements[getInputs().indexOf(source)] = EvaluableExpression.IDENTITY;
	// return new ArrayCreation(elements);
	// }

	@Override
	protected Operator createElementaryOperations(List<Operator> inputs) {
		if (inputs.size() <= 1)
			return new Projection(this.expression, new OneSourceAggregation(inputs.get(0)));

		List<Operator> aggreations = new ArrayList<Operator>();

		for (int index = 0; index < inputs.size(); index++) {
			EvaluableExpression[] elements = new EvaluableExpression[this.getInputs().size()];
			Arrays.fill(elements, EvaluableExpression.NULL);
			elements[index] = EvaluableExpression.SAME_VALUE;
			aggreations.add(new Projection(new ArrayCreation(elements), inputs.get(index)));
		}

		UnionAll union = new UnionAll(aggreations);
		return new Projection(this.expression, new Projection(new ArrayUnion(), new OneSourceAggregation(union)));
	}

	private static final class ArrayUnion extends EvaluableExpression {
		/**
		 * 
		 */
		private static final long serialVersionUID = -5358556436487835033L;

		@Override
		public JsonNode evaluate(JsonNode node, EvaluationContext context) {
			Iterator<JsonNode> arrays = node.iterator();
			ArrayNode mergedArray = JsonUtil.NODE_FACTORY.arrayNode();
			while (arrays.hasNext()) {
				JsonNode array = arrays.next();
				for (int index = 0; index < array.size(); index++) {
					if (mergedArray.size() <= index)
						mergedArray.add(JsonUtil.NODE_FACTORY.arrayNode());
					if (!array.get(index).isNull())
						((ArrayNode) mergedArray.get(index)).add(array.get(index));
				}
			}
			return mergedArray;
		}
	}

	public static class OneSourceAggregation extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 561729616462154707L;

		public OneSourceAggregation(JsonStream input) {
			super(input);
		}

		public static class Implementation extends
				SopremoReduce<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			public void reduce(PactJsonObject.Key key, Iterator<PactJsonObject> values,
					Collector<PactJsonObject.Key, PactJsonObject> out) {
				out.collect(key, new PactJsonObject(JsonUtil.wrapWithNode(true, values)));
			}
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.expression.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		Aggregation other = (Aggregation) obj;
		return this.expression.equals(other.expression);
	}

	@Override
	public String toString() {
		return String.format("%s to %s", super.toString(), this.expression);
	}

	// public static class TwoSourceAggregation extends ElementaryOperator {
	// public TwoSourceAggregation(JsonStream input1, JsonStream input2) {
	// super(input1, input2);
	// }
	//
	// public static class Implementation extends
	// SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
	// @Override
	// public void coGroup(PactJsonObject.Key key, Iterator<PactJsonObject> values1,
	// Iterator<PactJsonObject> values2, Collector<PactJsonObject.Key, PactJsonObject> out) {
	// out.collect(key, new PactJsonObject(JsonUtil.wrapWithNode(true, values1, values2)));
	// }
	// }
	// }
}
