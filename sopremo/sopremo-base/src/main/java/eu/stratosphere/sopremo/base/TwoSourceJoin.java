package eu.stratosphere.sopremo.base;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.util.Collections;
import java.util.List;

import eu.stratosphere.pact.common.plan.ContractUtil;
import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.join.AntiJoin;
import eu.stratosphere.sopremo.base.join.OuterJoin;
import eu.stratosphere.sopremo.base.join.OuterJoin.Mode;
import eu.stratosphere.sopremo.base.join.SemiJoin;
import eu.stratosphere.sopremo.base.join.ThetaJoin;
import eu.stratosphere.sopremo.base.join.TwoSourceJoinBase;
import eu.stratosphere.sopremo.expressions.ArrayCreation;
import eu.stratosphere.sopremo.expressions.BinaryBooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.expressions.JsonStreamExpression;
import eu.stratosphere.sopremo.operator.Name;
import eu.stratosphere.sopremo.operator.Property;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class TwoSourceJoin extends TwoSourceJoinBase<TwoSourceJoin> {
	private static final long serialVersionUID = 3299811281318600335L;

	private BinaryBooleanExpression condition = new ComparativeExpression(new InputSelection(0),
		ComparativeExpression.BinaryOperator.EQUAL, new InputSelection(1));

	private TwoSourceJoinBase<?> strategy;

	private boolean inverseInputs;

	private IntSet outerJoinSources = new IntOpenHashSet();

	/**
	 * Initializes TwoSourceJoin.
	 */
	public TwoSourceJoin() {
		this.chooseStrategy();
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		this.strategy.setResultProjection(getResultProjection());
		if (!this.outerJoinSources.isEmpty() && this.strategy instanceof OuterJoin) {
			((OuterJoin) this.strategy).withMode(
				this.outerJoinSources.contains(this.inverseInputs ? 1 : 0),
				this.outerJoinSources.contains(this.inverseInputs ? 0 : 1));
		}
		final PactModule pactModule = this.strategy.asPactModule(context);
		if (this.inverseInputs)
			ContractUtil.swapInputs(pactModule.getOutput(0).getInputs().get(0), 0, 1);
		return pactModule;
	}

	public BinaryBooleanExpression getCondition() {
		return this.condition;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.condition.hashCode();
		result = prime * result + (this.inverseInputs ? 1231 : 1237);
		result = prime * result + this.outerJoinSources.hashCode();
		result = prime * result + this.strategy.hashCode();
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		TwoSourceJoin other = (TwoSourceJoin) obj;
		return this.condition.equals(other.condition) && this.inverseInputs == other.inverseInputs
			&& this.outerJoinSources.equals(other.outerJoinSources) && this.strategy.equals(other.strategy);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.ElementaryOperator#getKeyExpressions(int)
	 */
	@Override
	public List<? extends EvaluationExpression> getKeyExpressions(int inputIndex) {
		return this.strategy.getKeyExpressions(inputIndex);
	}

	public ArrayCreation getOuterJoinSources() {
		EvaluationExpression[] expressions = new EvaluationExpression[this.outerJoinSources.size()];
		final IntIterator iterator = this.outerJoinSources.iterator();
		for (int index = 0; iterator.hasNext(); index++) {
			final int inputIndex = iterator.nextInt();
			expressions[index] = new JsonStreamExpression(getInput(inputIndex), inputIndex);
		}
		return new ArrayCreation(expressions);
	}

	public void setCondition(BinaryBooleanExpression condition) {
		if (condition == null)
			throw new NullPointerException("condition must not be null");

		EvaluationExpression expr1, expr2;
		if (condition instanceof ComparativeExpression) {
			expr1 = ((ComparativeExpression) condition).getExpr1();
			expr2 = ((ComparativeExpression) condition).getExpr2();
		} else if (condition instanceof ElementInSetExpression) {
			expr1 = ((ElementInSetExpression) condition).getElementExpr();
			expr2 = ((ElementInSetExpression) condition).getSetExpr();
		} else
			throw new IllegalArgumentException(String.format("Type of condition %s not supported",
				condition.getClass().getSimpleName()));

		int inputIndex1 = SopremoUtil.getInputIndex(expr1);
		int inputIndex2 = SopremoUtil.getInputIndex(expr2);
		if (inputIndex1 == inputIndex2)
			throw new IllegalArgumentException(String.format("Condition input selection is invalid %s", condition));
		else if (inputIndex1 < 0 || inputIndex1 > 1 || inputIndex2 < 0 || inputIndex2 > 1)
			throw new IllegalArgumentException(String.format("Condition input selection out of bounds %s", condition));
		this.condition = condition;
		this.chooseStrategy();
	}

	@Property
	@Name(verb = "preserve")
	public void setOuterJoinSources(EvaluationExpression outerJoinSources) {
		if (outerJoinSources == null)
			throw new NullPointerException("outerJoinSources must not be null");
		
		final Iterable<? extends EvaluationExpression> expressions;
		if (outerJoinSources instanceof JsonStreamExpression)
			expressions = Collections.singleton(outerJoinSources);
		else if (outerJoinSources instanceof ArrayCreation)
			expressions = ((ArrayCreation) outerJoinSources).getChildren();
		else
			throw new IllegalArgumentException(String.format("Cannot interpret %s", outerJoinSources));

		this.outerJoinSources.clear();
		for (EvaluationExpression expression : expressions)
			this.outerJoinSources.add(((JsonStreamExpression) expression).getInputIndex(getInputs()));
	}

	public void setOuterJoinIndices(int... outerJoinIndices) {
		if (outerJoinIndices == null)
			throw new NullPointerException("outerJoinIndices must not be null");
		
		this.outerJoinSources.clear();
		for (int index : outerJoinIndices)
			this.outerJoinSources.add(index);
	}
	
	public int[] getOuterJoinIndices() {
		return this.outerJoinSources.toIntArray();
	}

	public TwoSourceJoin withCondition(BinaryBooleanExpression condition) {
		this.setCondition(condition);
		return this;
	}

	public TwoSourceJoin withOuterJoinSources(EvaluationExpression outerJoinSources) {
		this.setOuterJoinSources(outerJoinSources);
		return this;
	}

	public TwoSourceJoin withOuterJoinIndices(int... outerJoinIndices) {
		this.setOuterJoinIndices(outerJoinIndices);
		return this;
	}

	/**
	 * Returns the strategy. For testing only.
	 * 
	 * @return the strategy
	 */
	TwoSourceJoinBase<?> getStrategy() {
		return this.strategy;
	}

	private void chooseStrategy() {
		this.inverseInputs = false;
		this.strategy = null;
		// choose the strategy, just probably generalized in a kind of factory
		if (this.condition instanceof ComparativeExpression) {
			ComparativeExpression comparison = (ComparativeExpression) this.condition.clone();
			switch (comparison.getBinaryOperator()) {
			case EQUAL:
				this.inverseInputs = comparison.getExpr1().find(InputSelection.class).getIndex() == 1;
				this.strategy = new OuterJoin().withMode(Mode.NONE).
					withKeyExpression(0, comparison.getExpr1().remove(InputSelection.class)).
					withKeyExpression(1, comparison.getExpr2().remove(InputSelection.class));
				break;
			default:
				this.strategy = new ThetaJoin().withComparison(comparison);
			}
		} else if (this.condition instanceof ElementInSetExpression) {
			ElementInSetExpression elementInSetExpression = (ElementInSetExpression) this.condition.clone();
			this.inverseInputs = elementInSetExpression.getElementExpr().find(InputSelection.class).getIndex() == 1;
			switch (elementInSetExpression.getQuantor()) {
			case EXISTS_NOT_IN:
				this.strategy = new AntiJoin().
					withKeyExpression(0, elementInSetExpression.getElementExpr().remove(InputSelection.class)).
					withKeyExpression(1, elementInSetExpression.getSetExpr().remove(InputSelection.class));
				break;
			case EXISTS_IN:
				this.strategy = new SemiJoin().
					withKeyExpression(0, elementInSetExpression.getElementExpr().remove(InputSelection.class)).
					withKeyExpression(1, elementInSetExpression.getSetExpr().remove(InputSelection.class));
				break;
			}
		}
		if (this.strategy == null)
			throw new UnsupportedOperationException("condition " + this.condition + " not supported");
	}
}
