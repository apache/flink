package eu.stratosphere.sopremo.base;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import eu.stratosphere.sopremo.CompositeOperator;
import eu.stratosphere.sopremo.ElementarySopremoModule;
import eu.stratosphere.sopremo.InputCardinality;
import eu.stratosphere.sopremo.JsonStream;
import eu.stratosphere.sopremo.Name;
import eu.stratosphere.sopremo.OutputCardinality;
import eu.stratosphere.sopremo.Property;
import eu.stratosphere.sopremo.SopremoModule;
import eu.stratosphere.sopremo.Source;
import eu.stratosphere.sopremo.expressions.AggregationExpression;
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.BinaryBooleanExpression;
import eu.stratosphere.sopremo.expressions.BooleanExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.util.IsInstancePredicate;

@InputCardinality(min = 2)
@OutputCardinality(1)
@Name(verb = "join")
public class Join extends CompositeOperator<Join> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6428723643579047169L;

	private BooleanExpression joinCondition = new AndExpression();

	private transient List<BinaryBooleanExpression> binaryConditions = new ArrayList<BinaryBooleanExpression>();

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
	public void setJoinCondition(BooleanExpression joinCondition) {
		if (joinCondition == null)
			throw new NullPointerException("joinCondition must not be null");

		final ArrayList<BinaryBooleanExpression> expressions = new ArrayList<BinaryBooleanExpression>();
		addBinaryExpressions(joinCondition, expressions);
		if (expressions.size() == 0)
			throw new IllegalArgumentException("No join condition given");

		this.joinCondition = joinCondition;
		this.binaryConditions = expressions;
	}

	public Join withResultProjection(EvaluationExpression resultProjection) {
		this.setResultProjection(resultProjection);
		return this;
	}

	public Join withJoinCondition(BooleanExpression joinCondition) {
		this.setJoinCondition(joinCondition);
		return this;
	}

	private void addBinaryExpressions(BooleanExpression joinCondition, List<BinaryBooleanExpression> expressions) {
		if (joinCondition instanceof BinaryBooleanExpression)
			expressions.add((BinaryBooleanExpression) joinCondition);
		else if (joinCondition instanceof AndExpression)
			for (BooleanExpression expression : ((AndExpression) joinCondition).getExpressions())
				addBinaryExpressions(expression, expressions);
		else
			throw new IllegalArgumentException("Cannot handle expression " + joinCondition);
	}

	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final int numInputs = this.getInputs().size();

		final SopremoModule module = new SopremoModule(this.toString(), numInputs, 1);

		switch (this.binaryConditions.size()) {
		case 0:
			throw new IllegalStateException("No join condition specified");
		case 1:
			// only two way join
			final TwoSourceJoin join = new TwoSourceJoin().
				withInputs(module.getInputs()).
				withCondition(this.binaryConditions.get(0)).
				withResultProjection(getResultProjection());
			module.getOutput(0).setInput(0, join);
			break;

		default:
			List<TwoSourceJoin> joins = this.getInitialJoinOrder(module);
			// return new TwoSourceJoin().
			// withCondition(this.joinCondition).
			// withInputs(getInputs()).
			// asElementaryOperators();

			final List<JsonStream> inputs = new ArrayList<JsonStream>();
			for (int index = 0; index < numInputs; index++) {
				inputs.add(OperatorUtil.positionEncode(module.getInput(index), index, numInputs));
			}

			for (final TwoSourceJoin twoSourceJoin : joins) {
				List<JsonStream> operatorInputs = twoSourceJoin.getInputs();

				final List<JsonStream> actualInputs = new ArrayList<JsonStream>(operatorInputs.size());
				List<Source> moduleInput = Arrays.asList(module.getInputs());
				for (int index = 0; index < operatorInputs.size(); index++) {
					final int inputIndex = moduleInput.indexOf(operatorInputs.get(index).getSource().getOperator());
					actualInputs.add(inputs.get(inputIndex).getSource());
				}
				for (int index = 0; index < operatorInputs.size(); index++) {
					final int inputIndex = moduleInput.indexOf(operatorInputs.get(index).getSource().getOperator());
					inputs.set(inputIndex, twoSourceJoin);
				}
				twoSourceJoin.setInputs(actualInputs);
				twoSourceJoin.setResultProjection(new AggregationExpression(new ArrayUnion()));
			}

			final TwoSourceJoin lastJoin = joins.get(joins.size() - 1);
			module.getOutput(0).setInput(0,
				new Projection().withInputs(lastJoin).withResultProjection(getResultProjection()));
		}

		return module.asElementary();
	}

	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		return super.equals(obj) && this.joinCondition.equals(((Join) obj).joinCondition)
			&& this.resultProjection.equals(((Join) obj).resultProjection);
	}

	@Override
	public String toString() {
		final StringBuilder builder = new StringBuilder(this.getName());
		builder.append(" on ").append(this.getJoinCondition());
		if (this.getResultProjection() != EvaluationExpression.VALUE)
			builder.append(" to ").append(this.getResultProjection());
		return builder.toString();
	}

	public BooleanExpression getJoinCondition() {
		return this.joinCondition;
	}

	private List<TwoSourceJoin> getInitialJoinOrder(SopremoModule module) {
		final List<TwoSourceJoin> joins = new ArrayList<TwoSourceJoin>();
		for (final BinaryBooleanExpression expression : this.binaryConditions)
			joins.add(this.getTwoSourceJoinForExpression(expression, module));

		// TODO: add some kind of optimization?
		return joins;
	}

	private TwoSourceJoin getTwoSourceJoinForExpression(final BinaryBooleanExpression binaryCondition,
			SopremoModule module) {
		List<EvaluationExpression> inputSelections = binaryCondition.findAll(new IsInstancePredicate(InputSelection.class));
		if (inputSelections.size() != 2)
			throw new IllegalArgumentException(String.format("Condition must refer to two source: %s", binaryCondition));

		BinaryBooleanExpression adjustedExpression = (BinaryBooleanExpression) binaryCondition.clone();
		final int firstIndex = ((InputSelection) inputSelections.get(0)).getIndex();
		final int secondIndex = ((InputSelection) inputSelections.get(1)).getIndex();
		adjustedExpression.replace(inputSelections.get(0), new PathExpression(new InputSelection(0), new ArrayAccess(
			firstIndex)));
		adjustedExpression.replace(inputSelections.get(1), new PathExpression(new InputSelection(1), new ArrayAccess(
			secondIndex)));
		return new TwoSourceJoin().
			withInputs(module.getInput(firstIndex), module.getInput(secondIndex)).
			withCondition(adjustedExpression);
	}

	@Override
	public int hashCode() {
		final int prime = 37;
		int result = super.hashCode();
		result = prime * result + this.joinCondition.hashCode();
		result = prime * result + this.resultProjection.hashCode();
		return result;
	}

}
