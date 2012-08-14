package eu.stratosphere.sopremo.serialization;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.testing.PactRecordEqualer;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

public class ObjectSchemaTest {

	@Test
	public void conversionShouldKeepIdentity() {
		final ObjectSchema schema = new ObjectSchema("firstname", "lastname");
		final ObjectNode object = new ObjectNode();
		object.put("firstname", TextNode.valueOf("testfn"))
			.put("lastnameasdf", TextNode.valueOf("testln123"));

		final PactRecord record = schema.jsonToRecord(object, null, null);

		final IJsonNode object2 = schema.recordToJson(record, null);

		Assert.assertEquals(object, object2);
	}

	@Test
	public void shouldConvertFromJsonToRecord() {
		final ObjectSchema schema = new ObjectSchema("firstname", "lastname");

		final ObjectNode object = new ObjectNode();
		object.put("firstname", TextNode.valueOf("testfn"))
			.put("lastname", TextNode.valueOf("testln"));

		final PactRecord result = schema.jsonToRecord(object, null, null);
		final PactRecord expected = new PactRecord();
		expected.setField(0, new JsonNodeWrapper(TextNode.valueOf("testfn")));
		expected.setField(1, new JsonNodeWrapper(TextNode.valueOf("testln")));
		expected.setField(2, new JsonNodeWrapper(new ObjectNode()));

		Assert.assertTrue(PactRecordEqualer.recordsEqual(expected, result, schema.getPactSchema()));
	}

	@Test
	public void shouldConvertFromRecordToJson() {
		final ObjectSchema schema = new ObjectSchema("firstname", "lastname");

		final PactRecord record = new PactRecord();
		record.setField(0, new JsonNodeWrapper(TextNode.valueOf("testfn")));
		record.setField(1, new JsonNodeWrapper(TextNode.valueOf("testln")));
		record.setField(2, new JsonNodeWrapper(new ObjectNode()));

		final IJsonNode result = schema.recordToJson(record, null);
		final IJsonNode expected = new ObjectNode().put("firstname", TextNode.valueOf("testfn"))
			.put("lastname", TextNode.valueOf("testln"));

		Assert.assertEquals(expected, result);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void shouldReturnObjectAsRecordWithMissingSchema() {
		final ObjectSchema schema = new ObjectSchema();
		final ObjectNode object = new ObjectNode().put("firstname", TextNode.valueOf("testfn"))
			.put("lastname", TextNode.valueOf("testln"));

		final PactRecord result = schema.jsonToRecord(object, null, null);

		final PactRecord expected = new PactRecord();
		expected.setField(0, new JsonNodeWrapper(object));

		Assert.assertTrue(PactRecordEqualer.recordsEqual(expected, result, new Class[] { JsonNodeWrapper.class }));
	}

	@Test
	public void shouldReturnObjectNodeWithMissingSchema() {
		final ObjectSchema schema = new ObjectSchema();
		final PactRecord record = new PactRecord();
		final ObjectNode object = new ObjectNode().put("firstName", TextNode.valueOf("Hans"))
			.put("lastName", TextNode.valueOf("Maier")).put("age", IntNode.valueOf(23));
		record.setField(0, new JsonNodeWrapper(object));
		final IJsonNode result = schema.recordToJson(record, null);
		Assert.assertEquals(object, result);
	}

	@Test(expected = IllegalStateException.class)
	public void shouldThrowExceptionWhenSchemaAndRecordDontMatch() {
		final ObjectSchema schema = new ObjectSchema("firstname", "lastname");

		final PactRecord record = new PactRecord();
		record.setField(0, new JsonNodeWrapper(TextNode.valueOf("testfn")));
		schema.recordToJson(record, null);
	}

	@Test
	public void shouldUseJsonNodeTarget() {
		final ObjectSchema schema = new ObjectSchema("firstname", "lastname");

		final PactRecord record = new PactRecord();
		record.setField(0, new JsonNodeWrapper(TextNode.valueOf("testfn")));
		record.setField(1, new JsonNodeWrapper(TextNode.valueOf("testln")));
		record.setField(2, new JsonNodeWrapper(new ObjectNode()));

		final IJsonNode target = new ObjectNode();
		final IJsonNode result = schema.recordToJson(record, target);

		Assert.assertSame(target, result);
	}

	@Test
	public void shouldUseRecordTarget() {
		final ObjectSchema schema = new ObjectSchema("firstname", "lastname");

		final ObjectNode object = new ObjectNode().put("firstname", TextNode.valueOf("testfn"))
			.put("lastname", TextNode.valueOf("testln"));

		final PactRecord target = schema.jsonToRecord(object, null, null);
		final PactRecord result = schema.jsonToRecord(object, target, null);

		Assert.assertSame(target, result);
	}
}
