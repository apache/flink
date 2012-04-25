package eu.stratosphere.sopremo.expressions;

import junit.framework.Assert;
import nl.jqno.equalsverifier.EqualsVerifier;
import nl.jqno.equalsverifier.Warning;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.SopremoTest;

@Ignore
public abstract class EvaluableExpressionTest<T extends EvaluationExpression> extends SopremoTest<T> {
	protected EvaluationContext context;

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.SopremoTest#shouldComplyEqualsContract()
	 */
	@Override
	@Test
	public void shouldComplyEqualsContract() {
		super.shouldComplyEqualsContract();
	}

	@Before
	public void initContext() {
		this.context = new EvaluationContext();
	}

	@Override
	protected void initVerifier(final EqualsVerifier<T> equalVerifier) {
		super.initVerifier(equalVerifier);
		equalVerifier.suppress(Warning.TRANSIENT_FIELDS);
	}

	@Test
	public void testToString() {
		final StringBuilder builder = new StringBuilder();
		this.first.toString(builder);
		Assert.assertFalse(
			"builder did not write anything - override this test if it is indeed the desired behavior",
			builder.length() == 0);
	}
}
