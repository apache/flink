package eu.stratosphere.sopremo.serialization;

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;

import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.ConstantExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;

public class GeneralSchemaTest {

	private ArrayNode array;

	private GeneralSchema schema;

	private List<EvaluationExpression> mappings;

	private EvaluationContext context;

	@Before
	public void setUp() {
		this.mappings = new LinkedList<EvaluationExpression>();

		this.mappings.add(new ArithmeticExpression(new ArrayAccess(0),
			ArithmeticExpression.ArithmeticOperator.ADDITION,
			new ArrayAccess(1)));
		this.mappings.add(new ArithmeticExpression(new ArrayAccess(0),
			ArithmeticExpression.ArithmeticOperator.MULTIPLICATION, new ArrayAccess(1)));

		this.array = new ArrayNode(IntNode.valueOf(3), IntNode.valueOf(4));
		this.schema = new GeneralSchema(this.mappings);
		this.context = new EvaluationContext();
	}

	@Test
	public void shouldCreateACorrectRecord() {
		final PactRecord record = this.schema.jsonToRecord(this.array, null, this.context);

		Assert.assertEquals(IntNode.valueOf(7), record.getField(0, JsonNodeWrapper.class).getValue());
		Assert.assertEquals(IntNode.valueOf(12), record.getField(1, JsonNodeWrapper.class).getValue());
		Assert.assertEquals(this.array, record.getField(2, JsonNodeWrapper.class).getValue());
	}

	@Test(expected = IllegalArgumentException.class)
	public void shouldFailIfExpressionIsNotFound() {
		this.schema.indicesOf(new ConstantExpression(IntNode.valueOf(42)));
	}

	@Test
	public void shouldProvidePactSchemaWhenMappingsIsEmpty() {
		this.schema = new GeneralSchema();

		Assert.assertTrue(this.schema.getMappings().size() == 0);
		final Class<? extends Value>[] pactSchema = this.schema.getPactSchema();
		Assert.assertTrue(pactSchema.length == 1);
	}

	@Test
	public void shouldProvideTheCorrectIndex() {
		final IntSet expected = IntSets.singleton(1);
		final EvaluationExpression exp = new ArithmeticExpression(new ArrayAccess(0),
			ArithmeticExpression.ArithmeticOperator.MULTIPLICATION, new ArrayAccess(1));

		final IntSet result = this.schema.indicesOf(exp);

		// has to do this workaround because Assert.assertEquals(expected, result); fails
		Assert.assertEquals(expected, result);
	}

	@Test
	public void shouldProvideTheCorrectPactSchema() {
		final int count = 3;
		@SuppressWarnings("unchecked")
		final Class<? extends Value>[] expected = new Class[count];
		for (int i = 0; i < count; i++)
			expected[i] = JsonNodeWrapper.class;
		final Class<? extends Value>[] result = this.schema.getPactSchema();

		// has to do this workaround because Assert.assertEquals(expected, result); fails
		Assert.assertEquals(expected.length, result.length);
		for (int i = 0; i < expected.length; i++)
			Assert.assertEquals(expected[i], result[i]);

	}

	@Test
	public void shouldReturnTheCorrectNode() {
		final PactRecord record = new PactRecord(3);
		record.setField(2, this.array);

		Assert.assertEquals(this.array, this.schema.recordToJson(record, null));
	}

	@Test
	public void shouldUseTargetNodeIfProvided() {
		final PactRecord record = new PactRecord(3);
		record.setField(2, this.array);
		final ArrayNode target = new ArrayNode();

		final IJsonNode result = this.schema.recordToJson(record, target);

		Assert.assertSame(target, result);
		Assert.assertEquals(this.array, result);
	}

	@Test
	public void shouldUseTargetRecordIfProvided() {
		final PactRecord target = new PactRecord(3);

		final PactRecord record = this.schema.jsonToRecord(this.array, target, this.context);

		Assert.assertSame(target, record);
	}
}
