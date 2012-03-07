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

import org.junit.Before;
import org.junit.Test;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.testing.PactRecordEqualer;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IntNode;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class HeadArraySchemaTest {

	private HeadArraySchema schema;

	@Before
	public void setUp() {
		this.schema = new HeadArraySchema();
	}

	@Test
	public void shouldConvertFromJsonToRecord() {
		this.schema.setHeadSize(2);
		IArrayNode array = new ArrayNode();
		array.add(IntNode.valueOf(1));
		PactRecord result = this.schema.jsonToRecord(array, null);

		PactRecord expected = new PactRecord(3);
		expected.setField(0, SopremoUtil.wrap(IntNode.valueOf(1)));
		expected.setField(2, SopremoUtil.wrap(new ArrayNode()));

		Assert.assertTrue(PactRecordEqualer.recordsEqual(expected, result, this.schema.getPactSchema()));
	}

	@Test
	public void shouldConvertFromRecordToJson() {
		PactRecord record = new PactRecord();
		this.schema.setHeadSize(2);

		record.setField(0, SopremoUtil.wrap(IntNode.valueOf(0)));
		record.setField(1, SopremoUtil.wrap(IntNode.valueOf(1)));
		record.setField(2, SopremoUtil.wrap(new ArrayNode(IntNode.valueOf(2))));

		IArrayNode expected = new ArrayNode(IntNode.valueOf(0), IntNode.valueOf(1), IntNode.valueOf(2));
		IJsonNode result = this.schema.recordToJson(record, null);

		Assert.assertEquals(expected, result);
	}

	@Test
	public void shouldKeepIdentityOnConversion() {
		PactRecord record = new PactRecord();
		this.schema.setHeadSize(5);

		record.setField(0, SopremoUtil.wrap(IntNode.valueOf(0)));
		record.setField(1, SopremoUtil.wrap(IntNode.valueOf(1)));
		record.setField(5, SopremoUtil.wrap(new ArrayNode()));

		IJsonNode node = this.schema.recordToJson(record, null);
		PactRecord result = this.schema.jsonToRecord(node, null);

		Assert.assertTrue(PactRecordEqualer.recordsEqual(record, result, this.schema.getPactSchema()));
	}
}
