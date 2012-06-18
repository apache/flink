package eu.stratosphere.sopremo.base;

import eu.stratosphere.pact.common.plan.PactModule;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.ExpressionTag;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.base.join.AntiJoin;
import eu.stratosphere.sopremo.base.join.OuterJoin;
import eu.stratosphere.sopremo.base.join.SemiJoin;
import eu.stratosphere.sopremo.base.join.ThetaJoin;
import eu.stratosphere.sopremo.base.join.TwoSourceJoinBase;
import eu.stratosphere.sopremo.expressions.BinaryBooleanExpression;
import eu.stratosphere.sopremo.expressions.ComparativeExpression;
import eu.stratosphere.sopremo.expressions.ElementInSetExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.InputSelection;
import eu.stratosphere.sopremo.pact.SopremoUtil;

public class TwoSourceJoin extends TwoSourceJoinBase<TwoSourceJoin> {
	private static final long serialVersionUID = 3299811281318600335L;

	private BinaryBooleanExpression condition;

	public BinaryBooleanExpression getCondition() {
		return this.condition;
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
	}

	public TwoSourceJoin withCondition(BinaryBooleanExpression condition) {
		this.setCondition(condition);
		return this;
	}

	@Override
	public PactModule asPactModule(EvaluationContext context) {
		Operator<?> strategy = this.chooseStrategy();
		strategy.setInputs(this.getInputs());
		return strategy.asPactModule(context);
	}

	private Operator<?> chooseStrategy() {
		// choose the strategy, just probably generalized in a kind of factory
		if (this.condition instanceof ComparativeExpression) {
			ComparativeExpression comparison = (ComparativeExpression) this.condition;
			switch (comparison.getBinaryOperator()) {
			case EQUAL:
				EvaluationExpression[] expressions =
					sortExpressionsWithInput(comparison.getExpr1(), comparison.getExpr2());
				return new OuterJoin().
					withMode(expressions[0].find(InputSelection.class).hasTag(ExpressionTag.RETAIN),
						expressions[1].find(InputSelection.class).hasTag(ExpressionTag.RETAIN)).
					withKeyExpression(0, expressions[0].remove(InputSelection.class)).
					withKeyExpression(1, expressions[1].remove(InputSelection.class));

			default:
				return new ThetaJoin().withComparison(comparison);
			}
		} else if (this.condition instanceof ElementInSetExpression) {
			ElementInSetExpression elementInSetExpression = (ElementInSetExpression) this.condition;
			switch (elementInSetExpression.getQuantor()) {
			case EXISTS_NOT_IN:
				return new AntiJoin();
			case EXISTS_IN:
				return new SemiJoin();
			}
		}
		throw new UnsupportedOperationException("condition " + this.condition + " not supported");
	}

	private EvaluationExpression[] sortExpressionsWithInput(EvaluationExpression expr1, EvaluationExpression expr2) {
		int inputIndex1 = SopremoUtil.getInputIndex(expr1);
		int inputIndex2 = SopremoUtil.getInputIndex(expr2);
		if (inputIndex1 < inputIndex2)
			return new EvaluationExpression[] { expr1, expr2 };
		return new EvaluationExpression[] { expr2, expr1 };
	}
}
