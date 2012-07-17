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
package eu.stratosphere.sopremo.type;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import eu.stratosphere.pact.testing.AssertUtil;

/**
 * @author Michael Hopstock
 */
@Ignore
public abstract class ObjectNodeBaseTest<T extends IObjectNode> extends JsonNodeTest<T> {

	// protected T node;

	@Override
	@Before
	public void setUp() {
	}

	@Before
	public void initObjectNode() {
		this.node = createObjectNode();
	}

	public abstract T createObjectNode();

	@Test
	public void shouldSetAndGetValue() {
		this.node.put("key", IntNode.valueOf(42));
		Assert.assertEquals(IntNode.valueOf(42), this.node.get("key"));
	}

	@Test
	public void shouldHaveCorrectSize() {
		this.node.clear();
		Assert.assertEquals(0, this.node.size());
		this.node.put("key1", IntNode.valueOf(23)).put("key2", IntNode.valueOf(42));
		Assert.assertEquals(2, this.node.size());
	}

	@Test
	public void shouldReturnMissingNodeIfFieldNotSet() {
		Assert.assertSame(MissingNode.getInstance(), this.node.get("thisFieldShouldNotBeAssigned"));
	}

	@Test
	public void shouldRemoveNode() {
		this.node.put("testkey", NullNode.getInstance());
		Assert.assertSame(NullNode.getInstance(), this.node.remove("testkey"));
		Assert.assertSame(MissingNode.getInstance(), this.node.get("testkey"));
	}

	@Test
	public void shouldCreateIterator() {
		this.node.clear();
		Map<String, IJsonNode> expected = new TreeMap<String, IJsonNode>();

		for (int i = 0; i < 5; i++) {
			String key = "key" + i;
			IJsonNode value = IntNode.valueOf(i);

			expected.put(key, value);
			this.node.put(key, value);
		}

		Iterator<Entry<String, IJsonNode>> it = this.node.iterator();
		AssertUtil.assertIteratorEquals(expected.entrySet().iterator(), it);
	}

	@Test
	public void shouldPutAll() {
		Assert.assertEquals(this.node, this.node.putAll(this.node));
	}

	@Test
	public void shouldBeEqualWithAnotherObjectNode() {
		Assert.assertEquals(this.createObjectNode(), this.createObjectNode());
	}
}
