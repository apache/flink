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

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Michael Hopstock
 *
 */
@Ignore
public abstract class ObjectNodeBaseTest<T extends IObjectNode>{
	
	T node;
	
	@Before
	public abstract void initObjectNode();
	
	@Test
	public void shouldSetAndGetValue(){
		node.put("key", IntNode.valueOf(42));
		Assert.assertEquals(IntNode.valueOf(42), node.get("key"));
	}
	
	@Test
	public void shouldHaveCorrectSize(){
		node.removeAll();
		Assert.assertEquals(0, node.size());
		node.put("key1", IntNode.valueOf(23)).put("key2", IntNode.valueOf(42));
		Assert.assertEquals(2, node.size());
	}

}
