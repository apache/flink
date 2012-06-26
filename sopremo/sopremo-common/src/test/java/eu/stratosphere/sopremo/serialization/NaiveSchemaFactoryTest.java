package eu.stratosphere.sopremo.serialization;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression.ArithmeticOperator;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.ObjectAccess;
import eu.stratosphere.sopremo.type.IntNode;

public class NaiveSchemaFactoryTest {

	private NaiveSchemaFactory factory;

	@Before
	public void setUp() {
		this.factory = new NaiveSchemaFactory();
	}

	@Test
	public void shouldCreateObjectSchema() {
		List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();
		accesses.add(new ObjectAccess("firstname"));
		accesses.add(new ObjectAccess("lastname"));
		accesses.add(new ObjectAccess("age"));

		Schema schema = this.factory.create(accesses);

		Assert.assertTrue(schema instanceof ObjectSchema);
	}

	@Test
	public void shouldCreateHeadArraySchema() {
		List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();
		accesses.add(new ArrayAccess(0));
		accesses.add(new ArrayAccess(1));
		accesses.add(new ArrayAccess(2));
		accesses.add(new ArrayAccess(3));

		Schema schema = this.factory.create(accesses);

		Assert.assertTrue(schema instanceof HeadArraySchema);
		Assert.assertEquals(4, ((HeadArraySchema) schema).getHeadSize());
	}

	@Test
	public void shouldCreateTailArraySchema() {
		List<EvaluationExpression> accesses = new ArrayList<EvaluationExpression>();
		accesses.add(new ArrayAccess(7));
		accesses.add(new ArrayAccess(8));
		accesses.add(new ArrayAccess(9));
		accesses.add(new ArrayAccess(10));

		Schema schema = this.factory.create(accesses);

		Assert.assertTrue(schema instanceof TailArraySchema);
		Assert.assertEquals(4, ((TailArraySchema) schema).getTailSize());
	}

	@Test
	public void shouldCreateDirectSchema() {
		List<EvaluationExpression> expressions = new ArrayList<EvaluationExpression>();

		Schema schema = this.factory.create(expressions);

		Assert.assertTrue(schema instanceof DirectSchema);
	}

	@Test
	public void shouldCreateGeneralSchema() {
		List<EvaluationExpression> expressions = new ArrayList<EvaluationExpression>();
		expressions.add(new ConstantExpression(IntNode.valueOf(42)));
		expressions.add(new ArithmeticExpression(new ArrayAccess(0), ArithmeticOperator.ADDITION, new ArrayAccess(1)));

		Schema schema = this.factory.create(expressions);

		Assert.assertTrue(schema instanceof GeneralSchema);
	}
}
