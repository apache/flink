package eu.stratosphere.sopremo.expressions;

import org.eclipse.jetty.util.ajax.JSONPojoConvertor.NumberType;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationException;
import eu.stratosphere.sopremo.jsondatamodel.TextNode;
import eu.stratosphere.sopremo.jsondatamodel.BooleanNode;
public class NumberCastingExpressionTest extends EvaluableExpressionTest<NumberCastingExpression> {

	@Override
	protected NumberCastingExpression createDefaultInstance(int index) {
		switch (index) {
		case 0: {
			return new NumberCastingExpression(NumberType.INT);
		}
		case 1: {
			return new NumberCastingExpression(NumberType.FLOAT);
		}
		case 2: {
			return new NumberCastingExpression(NumberType.BIG_INTEGER);
		}
		}
		return new NumberCastingExpression(NumberType.INT);
	}

	@Test(expected = EvaluationException.class)
	public void shouldThrowExceptionWhenCastingNoNumeric() {
		new NumberCastingExpression(NumberType.INT).evaluate(BooleanNode.TRUE, this.context);
	}

	@Test(expected = EvaluationException.class)
	public void shouldThrowExceptionWhenCastingStringRepresentation(){
		new NumberCastingExpression(NumberType.DOUBLE).evaluate(TextNode.valueOf("42"), this.context);
	}
}
