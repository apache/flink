package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javassist.expr.NewArray;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.BooleanNode;
import org.codehaus.jackson.node.NullNode;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementaryOperator;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.ExpressionTag;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.JsonUtil;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.StreamArrayNode;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.ArrayMerger;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ContainerExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression.Quantor;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.PactJsonObject;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.pact.SopremoCross;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.pact.SopremoUtil;

@Name(verb = "join")
public class Join extends CompositeOperator {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6428723643579047169L;

	private EvaluationExpression condition = new ConstantExpression(true);

	private EvaluationExpression resultProjection = ObjectCreation.CONCATENATION;

	public EvaluationExpression getResultProjection() {
		return this.resultProjection;
	}

	@Property
	@Name(preposition = "into")
	public void setResultProjection(EvaluationExpression resultProjection) {
		if (resultProjection == null)
			throw new NullPointerException("resultProjection must not be null");

		this.resultProjection = resultProjection;
	}

	@Property
	@Name(preposition = "where")
	public void setJoinCondition(EvaluationExpression joinCondition) {
		if (joinCondition == null)
			throw new NullPointerException("joinCondition must not be null");

		this.condition = joinCondition;
	}

	public Join withResultProjection(EvaluationExpression resultProjection) {
		setResultProjection(resultProjection);
		return this;
	}

	public Join withJoinCondition(EvaluationExpression joinCondition) {
		setJoinCondition(joinCondition);
		return this;
	}

	@Override
	public SopremoModule asElementaryOperators() {

		final int numInputs = this.getInputs().size();
		final SopremoModule module = new SopremoModule(this.toString(), numInputs, 1);

		List<TwoSourceJoin> joins;
		if (this.condition instanceof AndExpression)
			joins = this.getInitialJoinOrder((AndExpression) this.condition, module);
		else
			joins = Arrays.asList(this.getTwoSourceJoinForExpression(this.condition, module));

		final List<Operator> inputs = new ArrayList<Operator>();
		for (int index = 0; index < numInputs; index++) {
			final EvaluationExpression[] elements = new EvaluationExpression[numInputs];
			Arrays.fill(elements, EvaluationExpression.NULL);
			elements[index] = EvaluationExpression.VALUE;
			inputs.add(new Projection().
				withValueTransformation(new ArrayCreation(elements)).
				withInputs(module.getInput(index)));
		}

		for (final TwoSourceJoin twoSourceJoin : joins) {
			final List<Output> operatorInputs = twoSourceJoin.getInputs();
			final Output[] actualInputs = new Output[2];
			List<Source> moduleInput = Arrays.asList(module.getInputs());
			for (int index = 0; index < operatorInputs.size(); index++) {
				final int inputIndex = moduleInput.indexOf(operatorInputs.get(index).getOperator());
				actualInputs[index] = inputs.get(inputIndex).getSource();
			}
			for (int index = 0; index < operatorInputs.size(); index++) {
				final int inputIndex = moduleInput.indexOf(operatorInputs.get(index).getOperator());
				inputs.set(inputIndex, twoSourceJoin);
			}
			twoSourceJoin.setInputs(actualInputs);
		}

		module.getOutput(0).setInput(0, new Projection().
			withKeyTransformation(EvaluationExpression.NULL).
			withValueTransformation(this.resultProjection).
			withInputs(joins.get(joins.size() - 1)));
		return module;
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		return super.equals(obj) && this.condition.equals(((Join) obj).condition)
			&& this.resultProjection.equals(((Join) obj).resultProjection);
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" on ").append(this.getCondition());
		if (getResultProjection() != EvaluationExpression.VALUE)
			builder.append(" to ").append(getResultProjection());
		return builder.toString();
	}

	public EvaluationExpression getCondition() {
		return this.condition;
	}

	private List<TwoSourceJoin> getInitialJoinOrder(final AndExpression condition, SopremoModule module) {
		final List<TwoSourceJoin> joins = new ArrayList<TwoSourceJoin>();
		for (final EvaluationExpression expression : condition.getExpressions())
			joins.add(this.getTwoSourceJoinForExpression(expression, module));

		// TODO: add some kind of optimization
		return joins;
	}

	private int getInputIndex(final EvaluationExpression expr1) {
		return ((ContainerExpression) expr1).find(InputSelection.class).getIndex();
	}

	private TwoSourceJoin getTwoSourceJoinForExpression(final EvaluationExpression condition, SopremoModule module) {
		if (condition instanceof ComparativeExpression)
			return new ComparisonJoin(
				module.getInput(this.getInputIndex(((ComparativeExpression) condition).getExpr1())),
				module.getInput(this.getInputIndex(((ComparativeExpression) condition).getExpr2())),
				(ComparativeExpression) condition);
		if (condition instanceof ElementInSetExpression)
			return new ElementInSetJoin(
				module.getInput(this.getInputIndex(((ElementInSetExpression) condition).getElementExpr())),
				module.getInput(this.getInputIndex(((ElementInSetExpression) condition).getSetExpr())),
				(ElementInSetExpression) condition);
		throw new UnsupportedOperationException("condition " + condition + " not supported");
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result + this.condition.hashCode();
		result = prime * result + this.resultProjection.hashCode();
		return result;
	}

	@InputCardinality(min = 2, max = 2)
	public static class AntiJoinStub extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 2672827253341673832L;

		public static class Implementation extends
				SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void coGroup(final JsonNode key, final StreamArrayNode values1, final StreamArrayNode values2,
					final JsonCollector out) {
				if (values2.isEmpty())
					for (final JsonNode value : values1)
						out.collect(key, JsonUtil.asArray(value));
			}
		}
	}

	static class ComparisonJoin extends TwoSourceJoin {
		/**
		 * 
		 */
		private static final long serialVersionUID = -3045868252288937108L;

		private final ComparativeExpression comparison;

		public ComparisonJoin(final JsonStream left, final JsonStream right, final ComparativeExpression comparison) {
			super(left, right, comparison.getExpr1(), comparison.getExpr2());
			this.comparison = comparison;
		}

		@Override
		public Operator createJoinContract(final Operator left, final Operator right) {
			switch (this.comparison.getBinaryOperator()) {
			case EQUAL:
				final boolean leftOuter = this.getLeftJoinKey().removeTag(ExpressionTag.RETAIN);
				final boolean rightOuter = this.getRightJoinKey().removeTag(ExpressionTag.RETAIN);
				if (leftOuter || rightOuter)
					return new OuterJoinStub(leftOuter, rightOuter).withInputs(left, right);

				return new InnerJoinStub().withInputs(left, right);
			default:
				return new ThetaJoinStub(this.comparison).withInputs(left, right);
			}
		}
	}

	static class ElementInSetJoin extends TwoSourceJoin {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1650057142925592093L;

		private final ElementInSetExpression elementInSetExpression;

		public ElementInSetJoin(final JsonStream left, final JsonStream right,
				final ElementInSetExpression elementInSetExpression) {
			super(left, right, elementInSetExpression.getElementExpr(), elementInSetExpression.getSetExpr());
			this.elementInSetExpression = elementInSetExpression;
		}

		@Override
		public Operator createJoinContract(final Operator left, final Operator right) {
			if (this.elementInSetExpression.getQuantor() == Quantor.EXISTS_NOT_IN)
				return new AntiJoinStub().withInputs(left, right);
			return new SemiJoinStub().withInputs(left, right);
		}
	}

	@InputCardinality(min = 2, max = 2)
	public static class InnerJoinStub extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7145499293300473008L;

		public static class Implementation extends
				SopremoMatch<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void match(final JsonNode key, final JsonNode value1, final JsonNode value2,
					final JsonCollector out) {
				out.collect(key, JsonUtil.asArray(value1, value2));
			}
		}
	}

	@InputCardinality(min = 2, max = 2)
	public static class OuterJoinStub extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = 317168181417121979L;

		private transient boolean leftOuter, rightOuter;

		public OuterJoinStub(final boolean leftOuter, final boolean rightOuter) {
			this.leftOuter = leftOuter;
			this.rightOuter = rightOuter;
		}

		@Override
		protected void configureContract(final Contract contract, final Configuration configuration,
				final EvaluationContext context) {
			super.configureContract(contract, configuration, context);
			configuration.setBoolean("leftOuter", this.leftOuter);
			configuration.setBoolean("rightOuter", this.rightOuter);
		}

		public static class Implementation extends
				SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			private transient boolean leftOuter, rightOuter;

			@Override
			protected void coGroup(final JsonNode key, final StreamArrayNode values1, final StreamArrayNode values2,
					final JsonCollector out) {
				if (values1.isEmpty()) {
					// special case: no items from first source
					// emit all values of the second source
					if (this.rightOuter)
						for (final JsonNode value : values2)
							out.collect(key, JsonUtil.asArray(NullNode.getInstance(), value));
					return;
				}

				if (values2.isEmpty()) {
					// special case: no items from second source
					// emit all values of the first source
					if (this.leftOuter)
						for (final JsonNode value : values1)
							out.collect(key, JsonUtil.asArray(value, NullNode.getInstance()));
					return;
				}

				// TODO: use resettable iterator to avoid OOME
				final ArrayList<JsonNode> firstSourceNodes = new ArrayList<JsonNode>();
				for (final JsonNode value : values1)
					firstSourceNodes.add(value);

				for (final JsonNode secondSourceNode : values2)
					for (final JsonNode firstSourceNode : firstSourceNodes)
						out.collect(key, JsonUtil.asArray(firstSourceNode, secondSourceNode));
			}

			@Override
			public void configure(final Configuration parameters) {
				super.configure(parameters);
				this.leftOuter = parameters.getBoolean("leftOuter", false);
				this.rightOuter = parameters.getBoolean("rightOuter", false);
			}
		}
	}

	@InputCardinality(min = 2, max = 2)
	public static class SemiJoinStub extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -7624313431291367616L;

		public static class Implementation extends
				SopremoCoGroup<PactJsonObject.Key, PactJsonObject, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			@Override
			protected void coGroup(final JsonNode key, final StreamArrayNode values1, final StreamArrayNode values2,
					final JsonCollector out) {
				if (!values2.isEmpty())
					for (final JsonNode value : values1)
						out.collect(key, JsonUtil.asArray(value));
			}
		}
	}

	@InputCardinality(min = 2, max = 2)
	public static class ThetaJoinStub extends ElementaryOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -952011340895859983L;

		private final ComparativeExpression comparison;

		public ThetaJoinStub(final ComparativeExpression comparison) {
			this.comparison = comparison;
		}

		public static class Implementation
				extends
				SopremoCross<PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject, PactJsonObject.Key, PactJsonObject> {
			private ComparativeExpression comparison;

			@Override
			protected void cross(final JsonNode key1, final JsonNode value1, final JsonNode key2,
					final JsonNode value2, final JsonCollector out) {
				if (this.comparison.evaluate(JsonUtil.asArray(value1.get(0), value2.get(1)), this.getContext()) == BooleanNode.TRUE)
					out.collect(JsonUtil.asArray(key1, key2), JsonUtil.asArray(value1, value2));
			}
		}
	}

	@InputCardinality(min = 2, max = 2)
	static abstract class TwoSourceJoin extends CompositeOperator {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4192583790586928743L;

		private final EvaluationExpression leftJoinKey, rightJoinKey;

		public TwoSourceJoin(JsonStream left, JsonStream right, final EvaluationExpression leftJoinKey,
				final EvaluationExpression rightJoinKey) {
			setInputs(left, right);
			this.leftJoinKey = leftJoinKey;
			this.rightJoinKey = rightJoinKey;
		}

		@Override
		public SopremoModule asElementaryOperators() {
			final SopremoModule sopremoModule = new SopremoModule(this.toString(), 2, 1);

			final Operator leftProjection = new Projection().
				withKeyTransformation(this.leftJoinKey).
				withInputs(sopremoModule.getInput(0));
			final Operator rightProjection = new Projection()
				.withKeyTransformation(this.rightJoinKey).
				withInputs(sopremoModule.getInput(1));
			final Operator joinAlgorithm = this.createJoinContract(leftProjection, rightProjection);
			sopremoModule.getOutput(0).setInputs(new Projection().
				withValueTransformation(new ArrayMerger()).
				withInputs(joinAlgorithm));
			return sopremoModule;
		}

		public abstract Operator createJoinContract(Operator left, Operator right);

		public EvaluationExpression getLeftJoinKey() {
			return this.leftJoinKey;
		}

		public EvaluationExpression getRightJoinKey() {
			return this.rightJoinKey;
		}

	}

}
