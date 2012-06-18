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
import eu.stratosphere.sopremo.expressions.AndExpression;
import eu.stratosphere.sopremo.expressions.BinaryBooleanExpression;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.ObjectCreation;
import eu.stratosphere.util.IsInstancePredicate;

@InputCardinality(min = 2)
@OutputCardinality(1)
@Name(verb = "join")
public class Join extends CompositeOperator<Join> {
	/**
	 * 
	 */
	private static final long serialVersionUID = 6428723643579047169L;

	private EvaluationExpression joinCondition = new ConstantExpression(true);

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

		this.joinCondition = joinCondition;
	}

	public Join withResultProjection(EvaluationExpression resultProjection) {
		this.setResultProjection(resultProjection);
		return this;
	}

	public Join withJoinCondition(EvaluationExpression joinCondition) {
		this.setJoinCondition(joinCondition);
		return this;
	}

	@Override
	public ElementarySopremoModule asElementaryOperators() {
		final int numInputs = this.getInputs().size();

		final SopremoModule module = new SopremoModule(this.toString(), numInputs, 1);

		List<TwoSourceJoin> joins;
		if (this.joinCondition instanceof AndExpression)
			joins = this.getInitialJoinOrder((AndExpression) this.joinCondition, module);
		else
			joins = Arrays.asList(this.getTwoSourceJoinForExpression(this.joinCondition, module));

		if (joins.size() > 1) {
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
			}
		}

		module.getOutput(0).setInput(0, new Projection().
			withTransformation(this.resultProjection).
			withInputs(joins.get(joins.size() - 1)));
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

	public EvaluationExpression getJoinCondition() {
		return this.joinCondition;
	}

	private List<TwoSourceJoin> getInitialJoinOrder(final AndExpression condition, SopremoModule module) {
		final List<TwoSourceJoin> joins = new ArrayList<TwoSourceJoin>();
		for (final EvaluationExpression expression : condition.getExpressions())
			joins.add(this.getTwoSourceJoinForExpression(expression, module));

		// TODO: add some kind of optimization?
		return joins;
	}

	private TwoSourceJoin getTwoSourceJoinForExpression(final EvaluationExpression condition, SopremoModule module) {
		if (!(condition instanceof BinaryBooleanExpression))
			throw new IllegalArgumentException(String.format("Can only join over binary conditions: %s", condition));

		BinaryBooleanExpression binaryCondition = (BinaryBooleanExpression) condition;
		List<EvaluationExpression> inputSelections = condition.findAll(new IsInstancePredicate(InputSelection.class));
		if (inputSelections.size() != 2)
			throw new IllegalArgumentException(String.format("Condition must refer to two source: %s", condition));
		return new TwoSourceJoin().
			withCondition(binaryCondition).
			withInputs(module.getInput(((InputSelection) inputSelections.get(0)).getIndex()),
				module.getInput(((InputSelection) inputSelections.get(1)).getIndex()));
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
