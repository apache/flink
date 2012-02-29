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
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class ArraySchema implements Schema {

	// [ head, ArrayNode(others) ]

	/**
	 * 
	 */
	private static final long serialVersionUID = 4772055788210326536L;

	private int headSize;

	public void setHeadSize(int headSize) {
		this.headSize = headSize;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#getPactSchema()
	 */
	@Override
	public Class<? extends Value>[] getPactSchema() {
		Class<? extends Value>[] schema = new Class[mappingSize() + 1];

		for (int i = 0; i <= mappingSize() ; i++) {
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
		IArrayNode others;
		if (target == null) {

			// the last element is the field "others"
			target = new PactRecord(this.headSize + 1);
			others = new ArrayNode();
			target.setField(headSize, new JsonNodeWrapper(others));
		} else {
			//clear the others field if target was already used
			others = (IArrayNode) SopremoUtil.unwrap(target.getField(this.headSize, JsonNodeWrapper.class));
			others.clear();
		}

		IJsonNode arrayElement;
		for (int i = 0; i < this.headSize; i++) {
				arrayElement = ((IArrayNode) value).get(i);
				if(!arrayElement.isMissing()){
					target.setField(i, new JsonNodeWrapper(arrayElement));
				} else{
					target.setNull(i);				
			}
		}
		
		//if there are still remaining elements in the array we put insert them into the others field
		if (this.mappingSize() < ((IArrayNode) value).size()) {
			for (int i = this.headSize; i < ((IArrayNode) value).size(); i++){
				others.add(((IArrayNode)value).get(i));
			}
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
		if(this.mappingSize()+1 != record.getNumFields()){
			throw new IllegalStateException("Schema does not match to record!");
		}
		if(target == null){
			target = new ArrayNode();
		} else {
			((IArrayNode)target).clear();
		}
		IJsonNode node;
		for(int i = 0; i< this.mappingSize(); i++){
			((IArrayNode)target).add(SopremoUtil.unwrap(record.getField(i, JsonNodeWrapper.class)));
		}
		return target;

	}

	/**
	 * @return
	 */
	private int mappingSize() {
		return this.headSize;
	}

}
