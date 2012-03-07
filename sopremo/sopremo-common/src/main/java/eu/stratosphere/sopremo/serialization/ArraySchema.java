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
public class ArraySchema implements Schema {

	// [ head, ArrayNode(others), tail ]

	/**
	 * 
	 */
	private static final long serialVersionUID = 4772055788210326536L;

	private int headSize = 0;

	private int tailSize = 0;

	/**
	 * Returns the headSize.
	 * 
	 * @return the headSize
	 */
	public int getHeadSize() {
		return headSize;
	}

	/**
	 * Sets the headSize to the specified value.
	 * 
	 * @param headSize
	 *        the headSize to set
	 */
	public void setHeadSize(int headSize) {
		this.headSize = headSize;
	}

	/**
	 * Returns the tailSize.
	 * 
	 * @return the tailSize
	 */
	public int getTailSize() {
		return tailSize;
	}

	/**
	 * Sets the tailSize to the specified value.
	 * 
	 * @param tailSize
	 *        the tailSize to set
	 */
	public void setTailSize(int tailSize) {
		this.tailSize = tailSize;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#getPactSchema()
	 */
	@Override
	public Class<? extends Value>[] getPactSchema() {
		Class<? extends Value>[] schema = new Class[getHeadSize() + getTailSize() + 1];

		for (int i = 0; i <= getHeadSize() + getTailSize(); i++) {
			schema[i] = JsonNodeWrapper.class;
		}

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
			int[] indices = new int[this.getHeadSize() + 1];
			for (int index = 0; index < indices.length; index++)
				indices[index] = index;
			return indices;
		} else if (arrayExpression.isSelectingRange()) {
			int startIndex = arrayExpression.getStartIndex();
			int endIndex = arrayExpression.getEndIndex();
			if (startIndex < 0 || endIndex < 0)
				throw new UnsupportedOperationException("Tail indices are not supported yet");
			if (endIndex >= this.getHeadSize())
				throw new IllegalArgumentException("Target index is not in head");

			int[] indices = new int[endIndex - startIndex];
			for (int index = 0; index < indices.length; index++)
				indices[index] = startIndex + index;
			return indices;
		}
		int index = arrayExpression.getStartIndex();
		if (index >= this.getHeadSize())
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
	public PactRecord jsonToRecord(IJsonNode value, PactRecord target) {
		IArrayNode others;
		if (target == null || target.getNumFields() != 1) {

			// the last element is the field "others"
			target = new PactRecord(this.getHeadSize() + this.getTailSize() + 1);
			others = new ArrayNode();
			target.setField(this.getHeadSize(), SopremoUtil.wrap(others));
		} else {
			// clear the others field if target was already used
			others = (IArrayNode) SopremoUtil.unwrap(target.getField(this.getHeadSize(), JsonNodeWrapper.class));
			others.clear();
		}

		IJsonNode arrayElement;

		// fill the first headSize elements of the arraynode into the record
		for (int i = 0; i < this.getHeadSize(); i++) {
			arrayElement = ((IArrayNode) value).get(i);
			if (!arrayElement.isMissing()) {
				target.setField(i, SopremoUtil.wrap(arrayElement));
			} else { // incoming array is smaller than headSize
				target.setNull(i);
			}
		}

		if (this.getHeadSize() + this.getTailSize() < ((IArrayNode) value).size()) {
			// there are still remaining elements in the array we insert them into the others field untill the tail
			// begins
			for (int i = this.getHeadSize(); i < ((IArrayNode) value).size() - this.getTailSize(); i++) {
				others.add(((IArrayNode) value).get(i));
			}
			// fill the rest into tail
			for (int i = ((IArrayNode) value).size() - this.getTailSize(); i < ((IArrayNode) value).size(); i++) {
				arrayElement = ((IArrayNode) value).get(i);
				if (!arrayElement.isMissing()) {
					target.setField(i - others.size() - 1, SopremoUtil.wrap(arrayElement));
				}
				/*
				 * should not happen
				 * else { /
				 * }
				 */
			}

		} else { // tail would possibly not get filled entirely, so we don't need to fill the others field
			for (int i = this.getHeadSize(); i < ((IArrayNode) value).size(); i++) {
				arrayElement = ((IArrayNode) value).get(i);
				if (!arrayElement.isMissing()) {
					target.setField(i + 1, SopremoUtil.wrap(arrayElement));
				} else { // headSize < size(incoming array) < headSize+ tailSize
					target.setNull(i + 1);
				}
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
		if (this.getHeadSize() + this.getTailSize()+ 1 != record.getNumFields()) {
			throw new IllegalStateException("Schema does not match to record!");
		}
		if (target == null) {
			target = new ArrayNode();
		} else { // array was used
			((IArrayNode) target).clear();
		}
		JsonNodeWrapper recordElement;
		// insert head of record
		for (int i = 0; i < this.getHeadSize(); i++) {
			recordElement = record.getField(i, JsonNodeWrapper.class);
			if (recordElement != null) {
				((IArrayNode) target).add(SopremoUtil.unwrap(recordElement));
			}
		}
		// insert all elements from others
		((IArrayNode) target).addAll((IArrayNode) SopremoUtil.unwrap(record.getField(this.getHeadSize(),
			JsonNodeWrapper.class)));

		// insert tail of record
		for (int i = this.getHeadSize() + 1; i <= this.getHeadSize() + this.getTailSize(); i++) {
			recordElement = record.getField(i, JsonNodeWrapper.class);
			if (recordElement != null) {
				((IArrayNode) target).add(SopremoUtil.unwrap(recordElement));
			}
		}
		return target;

	}

}
