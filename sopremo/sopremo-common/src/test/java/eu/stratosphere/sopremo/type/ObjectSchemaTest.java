package eu.stratosphere.sopremo.type;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.testing.PactRecordEqualer;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;

public class ObjectSchemaTest {

	private ObjectSchema schema;

	@Before
	public void setUp() {
		this.schema = new ObjectSchema();
	}

	
	@Test
	public void shouldConvertFromJsonToRecord() {
		this.schema.setMappings("firstname", "lastname");

		ObjectNode object = new ObjectNode();
		object.put("firstname", TextNode.valueOf("testfn"))
			.put("lastname", TextNode.valueOf("testln"));

		PactRecord result = this.schema.jsonToRecord(object, null);
		PactRecord expected = new PactRecord();
		expected.setField(0, new JsonNodeWrapper(TextNode.valueOf("testfn")));
		expected.setField(1, new JsonNodeWrapper(TextNode.valueOf("testln")));

		Assert.assertTrue(PactRecordEqualer.recordsEqual(expected, result, this.schema.getPactSchema()));
	}

	@Test
	public void shouldConvertFromRecordToJson() {
		this.schema.setMappings("firstname", "lastname");

		PactRecord record = new PactRecord();
		record.setField(0, new JsonNodeWrapper(TextNode.valueOf("testfn")));
		record.setField(1, new JsonNodeWrapper(TextNode.valueOf("testln")));
		record.setField(2, new JsonNodeWrapper(new ObjectNode()));

		JsonNode result = this.schema.recordToJson(record, null);
		JsonNode expected = new ObjectNode().put("firstname", TextNode.valueOf("testfn"))
			.put("lastname", TextNode.valueOf("testln"));

		Assert.assertEquals(expected, result);
	}

	@Test
	public void shouldUseRecordTarget() {
		this.schema.setMappings("firstname", "lastname");

		ObjectNode object = new ObjectNode().put("firstname", TextNode.valueOf("testfn"))
			.put("lastname", TextNode.valueOf("testln"));

		PactRecord target = new PactRecord();
		PactRecord result = this.schema.jsonToRecord(object, target);

		Assert.assertSame(target, result);
	}

	@Test
	public void shouldUseJsonNodeTarget() {
		this.schema.setMappings("firstname", "lastname");

		PactRecord record = new PactRecord();
		record.setField(0, new JsonNodeWrapper(TextNode.valueOf("testfn")));
		record.setField(1, new JsonNodeWrapper(TextNode.valueOf("testln")));
		record.setField(2, new JsonNodeWrapper(new ObjectNode()));


		JsonNode target = new ObjectNode();
		JsonNode result = this.schema.recordToJson(record, target);

		Assert.assertSame(target, result);
	}


	@SuppressWarnings("unchecked")
	@Test
	public void shouldReturnObjectAsRecordWithMissingSchema() {
		ObjectNode object = new ObjectNode().put("firstname", TextNode.valueOf("testfn"))
			.put("lastname", TextNode.valueOf("testln"));

		PactRecord result = this.schema.jsonToRecord(object, null);

		PactRecord expected = new PactRecord();
		expected.setField(0, new JsonNodeWrapper(object));

		Assert.assertTrue(PactRecordEqualer.recordsEqual(expected, result, new Class[]{ JsonNodeWrapper.class}));
	}

	@Test
	public void shouldReturnObjectNodeWithMissingSchema() {
		PactRecord record = new PactRecord();
		ObjectNode object = new ObjectNode().put("firstName", TextNode.valueOf("Hans"))
			.put("lastName", TextNode.valueOf("Maier")).put("age", IntNode.valueOf(23));
		record.setField(
			0,
			new JsonNodeWrapper(object));
		JsonNode result = this.schema.recordToJson(record, null);
		Assert.assertEquals(object, result);
	}
	
	@Test(expected=IllegalStateException.class)
	public void shouldThrowExceptionWhenSchemaAndRecordDontMatch(){
		this.schema.setMappings("firstname", "lastname");

		PactRecord record = new PactRecord();
		record.setField(0, new JsonNodeWrapper(TextNode.valueOf("testfn")));
		this.schema.recordToJson(record, null);
	}
}
