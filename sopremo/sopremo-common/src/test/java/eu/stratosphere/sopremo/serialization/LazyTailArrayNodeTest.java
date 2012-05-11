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

import org.junit.Ignore;
import java.util.Arrays;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.ArrayNodeBaseTest;
import eu.stratosphere.sopremo.type.IntNode;

/**
 * @author Michael Hopstock
 */
@Ignore
@RunWith(Parameterized.class)
public class LazyTailArrayNodeTest extends ArrayNodeBaseTest<LazyTailArrayNode> {

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.ArrayNodeBaseTest#initArrayNode()
	 */
	
	public LazyTailArrayNodeTest(int tailSize) {
		TailArraySchema schema = new TailArraySchema();
		schema.setTailSize(tailSize);
		PactRecord record = schema.jsonToRecord(
			new ArrayNode(IntNode.valueOf(0), IntNode.valueOf(1), IntNode.valueOf(2)), null, null);

		this.node = new LazyTailArrayNode(record, schema);

	}

	@Test
	public void shouldSetNewNode() {
		Assert.assertEquals(IntNode.valueOf(2), this.node.set(2, IntNode.valueOf(3)));
		Assert.assertEquals(IntNode.valueOf(3), this.node.get(2));
		this.node.addAll(Arrays.asList(IntNode.valueOf(4), IntNode.valueOf(5), IntNode.valueOf(6)));
		Assert.assertEquals(IntNode.valueOf(0), this.node.set(0, IntNode.valueOf(7)));
		Assert.assertEquals(IntNode.valueOf(7), this.node.get(0));

	}
	@Parameters
	public static List<Object[]> combinations() {
		return Arrays.asList(new Object[][] {
			{ 0 },
			{ 1 },
			{ 2 },
			{ 3 },
			{ 4 },
			{ 5 },
			{ 10 },
			{ 20 },
			{ 50 },
			{ 100 }
		});
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.ArrayNodeBaseTest#initArrayNode()
	 */
	@Override
	public void initArrayNode() {
		
	}

}
