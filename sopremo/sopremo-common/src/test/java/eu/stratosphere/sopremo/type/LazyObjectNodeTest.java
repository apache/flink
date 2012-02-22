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

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.sopremo.serialization.LazyObjectNode;
import eu.stratosphere.sopremo.serialization.ObjectSchema;

/**
 * @author strato
 *
 */
public class LazyObjectNodeTest extends ObjectNodeBaseTest<LazyObjectNode> {

	/* (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.ObjectNodeBaseTest#initObjectNode()
	 */
	@Override
	public void initObjectNode() {
		ObjectSchema schema = new ObjectSchema();
		schema.setMappings("key1");
		PactRecord record = new PactRecord();
		schema.jsonToRecord(new ObjectNode(), record);
		this.node = new LazyObjectNode(record, schema);
		
	}
	
	

}
