package eu.stratosphere.sopremo.expressions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;
import nl.jqno.equalsverifier.EqualsVerifier;

import org.junit.Test;

import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.IJsonNode;

public class AndExpressionTest extends BooleanExpressionTest<AndExpression> {

	@Override
	protected AndExpression createDefaultInstance(final int index) {
		final BooleanExpression[] params = new BooleanExpression[index + 1];
		Arrays.fill(params, TRUE);
		return new AndExpression(params);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluableExpressionTest#initVerifier(nl.jqno.equalsverifier.EqualsVerifier)
	 */
	@Override
	protected void initVerifier(final EqualsVerifier<AndExpression> equalVerifier) {
		super.initVerifier(equalVerifier);
		equalVerifier.withPrefabValues(List.class, new ArrayList<Object>(), new ArrayList<EvaluationExpression>(
			Collections.singleton(TRUE)));
	}

	@Test
	public void shouldBeTrueIfAllExprAreTrue() {
		final IJsonNode result =
			new AndExpression(TRUE, BooleanExpression.ensureBooleanExpression(EvaluationExpression.VALUE), TRUE).
				evaluate(BooleanNode.TRUE, null, this.context);

		Assert.assertEquals(BooleanNode.TRUE, result);
	}

	@Test
	public void shouldBeFalseIfOneExprIsFalse() {
		final IJsonNode result =
			new AndExpression(TRUE, BooleanExpression.ensureBooleanExpression(EvaluationExpression.VALUE), TRUE).
				evaluate(BooleanNode.FALSE, null, this.context);

		Assert.assertEquals(BooleanNode.FALSE, result);
	}

}
