/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.serialization;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.ObjectNodeBaseTest;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class LazyObjectNodeTest extends ObjectNodeBaseTest<LazyObjectNode> {

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.ObjectNodeBaseTest#initObjectNode()
	 */

	@Override
	public LazyObjectNode createObjectNode() {
		final ObjectSchema schema = new ObjectSchema("firstName", "lastName", "age");
		final PactRecord record = schema.jsonToRecord(
			new ObjectNode().put("firstName", TextNode.valueOf("Hans")).put("age", IntNode.valueOf(25))
				.put("gender", TextNode.valueOf("male")), null, null);

		return new LazyObjectNode(record, schema);
	}

	@Test
	public void shouldPutIntoTheRightRecordField() {
		this.node.put("lastName", TextNode.valueOf("Wurst"));
		this.node.put("profession", TextNode.valueOf("Butcher"));
		final PactRecord rec = this.node.getJavaValue();
		// the lastname is the second element in the mapping
		final IJsonNode lastName = SopremoUtil.unwrap(rec.getField(1, JsonNodeWrapper.class));
		Assert.assertEquals(TextNode.valueOf("Wurst"), lastName);
		// 3 elements in the mapping -> others is the 4th field
		final IObjectNode others = (IObjectNode) SopremoUtil.unwrap(rec.getField(3, JsonNodeWrapper.class));
		Assert.assertEquals(TextNode.valueOf("Butcher"), others.get("profession"));
	}

	@Override
	public void testValue() {
	}

	@Override
	protected IJsonNode lowerNode() {
		final ObjectSchema schema = new ObjectSchema("firstName", "lastName", "age");
		final PactRecord record = schema.jsonToRecord(
			new ObjectNode().put("firstName", TextNode.valueOf("Hans")).put("age", IntNode.valueOf(25))
				.put("gender", TextNode.valueOf("female")), null, null);

		return new LazyObjectNode(record, schema);
	}

	@Override
	protected IJsonNode higherNode() {
		final ObjectSchema schema = new ObjectSchema("firstName", "lastName", "age");
		final PactRecord record = schema.jsonToRecord(
			new ObjectNode().put("firstName", TextNode.valueOf("Hans")).put("age", IntNode.valueOf(25))
				.put("gender", TextNode.valueOf("male")), null, null);

		return new LazyObjectNode(record, schema);
	}

	@Override
	@Test(expected = UnsupportedOperationException.class)
	public void shouldNormalizeKeys() {
		super.shouldNormalizeKeys();
	}
}
