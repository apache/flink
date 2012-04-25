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
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.ArrayAccess;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonNodeWrapper;
import eu.stratosphere.sopremo.pact.SopremoUtil;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 */
public class HeadArraySchema implements Schema {

	// [ head, ArrayNode(others) ]

	/**
	 * 
	 */
	private static final long serialVersionUID = 4772055788210326536L;

	private int headSize;

	public void setHeadSize(int headSize) {
		this.headSize = headSize;
	}

	public int getHeadSize() {
		return this.headSize;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#getPactSchema()
	 */
	@Override
	public Class<? extends Value>[] getPactSchema() {
		@SuppressWarnings("unchecked")
		Class<? extends Value>[] schema = new Class[this.getHeadSize() + 1];

		for (int i = 0; i <= this.getHeadSize(); i++)
			schema[i] = JsonNodeWrapper.class;

		return schema;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.serialization.Schema#indicesOf(eu.stratosphere.sopremo.expressions.EvaluationExpression)
	 */
	@Override
	public int[] indicesOf(EvaluationExpression expression) {
		ArrayAccess arrayExpression = (ArrayAccess) expression;

		if (arrayExpression.isSelectingAll()) {
			int[] indices = new int[this.headSize + 1];
			for (int index = 0; index < indices.length; index++)
				indices[index] = index;
			return indices;
		} else if (arrayExpression.isSelectingRange()) {
			int startIndex = arrayExpression.getStartIndex();
			int endIndex = arrayExpression.getEndIndex();
			if (startIndex < 0 || endIndex < 0)
				throw new UnsupportedOperationException("Tail indices are not supported yet");
			if (endIndex >= this.headSize)
				throw new IllegalArgumentException("Target index is not in head");

			int[] indices = new int[endIndex - startIndex];
			for (int index = 0; index < indices.length; index++)
				indices[index] = startIndex + index;
			return indices;
		}
		int index = arrayExpression.getStartIndex();
		if (index >= this.headSize)
			throw new IllegalArgumentException("Target index is not in head");
		else if (index < 0)
			throw new UnsupportedOperationException("Tail indices are not supported yet");
		return new int[] { index };
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#jsonToRecord(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.pact.common.type.PactRecord)
	 */
	@Override
	public PactRecord jsonToRecord(IJsonNode value, PactRecord target, EvaluationContext context) {
		IArrayNode others;
		if (target == null) {

			// the last element is the field "others"
			target = new PactRecord(this.headSize + 1);
			others = new ArrayNode();
			target.setField(this.headSize, SopremoUtil.wrap(others));
		} else {
			// clear the others field if target was already used
			others = (IArrayNode) SopremoUtil.unwrap(target.getField(this.headSize, JsonNodeWrapper.class));
			others.clear();
		}

		// fill the first headSize elements of the arraynode into the record
		IJsonNode arrayElement;
		for (int i = 0; i < this.headSize; i++) {
			arrayElement = ((IArrayNode) value).get(i);
			if (!arrayElement.isMissing())
				target.setField(i, SopremoUtil.wrap(arrayElement));
			else // incoming array is smaller than headSize
				target.setNull(i);
		}

		// if there are still remaining elements in the array we insert them into the others field
		if (this.getHeadSize() < ((IArrayNode) value).size())
			for (int i = this.headSize; i < ((IArrayNode) value).size(); i++)
				others.add(((IArrayNode) value).get(i));

		return target;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#recordToJson(eu.stratosphere.pact.common.type.PactRecord,
	 * eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode recordToJson(PactRecord record, IJsonNode target) {
		if (this.getHeadSize() + 1 != record.getNumFields())
			throw new IllegalStateException("Schema does not match to record!");
		if (target == null)
			target = new ArrayNode();
		} else // array was used
			((IArrayNode) target).clear();
		
		// insert head of record
		for (int i = 0; i < this.getHeadSize(); i++) {
			if (record.getField(i, JsonNodeWrapper.class) != null) {
				((IArrayNode) target).add(SopremoUtil.unwrap(record.getField(i, JsonNodeWrapper.class)));
			}
		}

		// insert all elements from others
		((IArrayNode) target).addAll((IArrayNode) SopremoUtil.unwrap(record.getField(this.getHeadSize(),
			JsonNodeWrapper.class)));

		return target;

	}

}
