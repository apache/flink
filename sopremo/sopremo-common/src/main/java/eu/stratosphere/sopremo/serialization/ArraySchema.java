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

import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.Value;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class ArraySchema implements Schema {

	// [ head, tail, ArrayNode(others) ]

	/**
	 * 
	 */
	private static final long serialVersionUID = 4772055788210326536L;

	private int headSize;

	private int tailSize;

	public void setHeadSize(int headSize) {
		this.headSize = headSize;
	}

	public void setTailSize(int tailSize) {
		this.tailSize = tailSize;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#getPactSchema()
	 */
	@Override
	public Class<? extends Value>[] getPactSchema() {
		Class<? extends Value>[] schema = new Class[mappingSize()];

		for (int i = 0; i < mappingSize(); i++) {
			schema[i] = JsonNodeWrapper.class;
		}

		return schema;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#jsonToRecord(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.pact.common.type.PactRecord)
	 */
	@Override
	public PactRecord jsonToRecord(IJsonNode value, PactRecord target) {
		if (target == null) {

			// the last element is the field "others"
			target = new PactRecord(this.headSize + this.tailSize + 1);
		}

		for (int i = 0; i < mappingSize(); i++) {
			if (i < this.headSize) {
				// traverse head
				target.setField(i, new JsonNodeWrapper(((IArrayNode) value).get(i)));
			} else {
				// traverse tail, insert them after head
				target.setField(
					i,
					new JsonNodeWrapper(((IArrayNode) value).get(((IArrayNode) value).size() - this.tailSize
						+ (i - this.headSize))));
			}

		}

		if (this.headSize + this.tailSize >= ((IArrayNode) value).size()) {
			target.setField(this.mappingSize(), new JsonNodeWrapper(new ArrayNode()));
		} else {

		}

		return target;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#recordToJson(eu.stratosphere.pact.common.type.PactRecord,
	 * eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode recordToJson(PactRecord record, IJsonNode target) {
		// TODO
		return target;

	}

	/**
	 * @return
	 */
	private int mappingSize() {
		return this.headSize + this.tailSize;
	}

}
