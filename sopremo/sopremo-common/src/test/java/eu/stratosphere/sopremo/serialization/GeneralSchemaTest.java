package eu.stratosphere.sopremo.serialization;

import java.util.LinkedList;
import java.util.List;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.ArithmeticExpression;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;

public class GeneralSchemaTest {

	private GeneralSchema schema;

	private EvaluationContext context;

	@Before
	public void setUp() {
		List<EvaluationExpression> mappings = new LinkedList<EvaluationExpression>();

		mappings.add(new ArithmeticExpression(new ArrayAccess(0), ArithmeticExpression.ArithmeticOperator.ADDITION,
			new ArrayAccess(1)));
		mappings.add(new ArithmeticExpression(new ArrayAccess(0),
			ArithmeticExpression.ArithmeticOperator.MULTIPLICATION, new ArrayAccess(1)));

		this.schema = new GeneralSchema();
		this.schema.setMappings(mappings);
		this.context = new EvaluationContext();
	}

	@Test
	public void shouldReturnTheCorrectNode() {
		PactRecord record = new PactRecord(3);
		ArrayNode array = new ArrayNode(IntNode.valueOf(3), IntNode.valueOf(4));
		record.setField(2, array);

		Assert.assertEquals(array, this.schema.recordToJson(record, null));
	}

	@Test
	public void shouldCreateACorrectRecord() {
		IJsonNode array = new ArrayNode(IntNode.valueOf(3), IntNode.valueOf(4));

		PactRecord record = this.schema.jsonToRecord(array, null, this.context);

		Assert.assertEquals(IntNode.valueOf(7), record.getField(0, JsonNodeWrapper.class));
		Assert.assertEquals(IntNode.valueOf(12), record.getField(1, JsonNodeWrapper.class));
		Assert.assertEquals(array, record.getField(2, JsonNodeWrapper.class));
	}

	@Test
	public void shouldUseTargetRecordIfProvided() {
		IJsonNode array = new ArrayNode(IntNode.valueOf(3), IntNode.valueOf(4));
		PactRecord target = new PactRecord(3);

		PactRecord record = this.schema.jsonToRecord(array, target, this.context);

		Assert.assertSame(target, record);
	}

}
