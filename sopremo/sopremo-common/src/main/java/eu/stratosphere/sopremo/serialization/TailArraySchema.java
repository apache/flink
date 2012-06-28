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

import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import eu.stratosphere.pact.common.type.PactRecord;
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
public class TailArraySchema extends AbstractSchema {

	// [ head, ArrayNode(others), tail ]

	/**
	 * 
	 */
	private static final long serialVersionUID = 4772055788210326536L;

	private int tailSize = 0;

	public TailArraySchema(final int tailSize) {
		super(tailSize + 1, rangeFrom(1, tailSize + 1));
		this.tailSize = tailSize;
	}

	/**
	 * Returns the tailSize.
	 * 
	 * @return the tailSize
	 */
	public int getTailSize() {
		return this.tailSize;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.serialization.Schema#indicesOf(eu.stratosphere.sopremo.expressions.EvaluationExpression)
	 */
	@Override
	public IntSet indicesOf(final EvaluationExpression expression) {
		// TODO check correctness
		final ArrayAccess arrayExpression = (ArrayAccess) expression;

		if (arrayExpression.isSelectingAll())
			return rangeFrom(0, this.tailSize + 1);
		else if (arrayExpression.isSelectingRange()) {
			int startIndex = arrayExpression.getStartIndex();
			int endIndex = arrayExpression.getEndIndex();
			if (startIndex >= 0 || endIndex >= 0)
				throw new UnsupportedOperationException("Head indices are not supported yet");
			endIndex += this.tailSize;
			startIndex += this.tailSize;
			if (endIndex >= this.getTailSize())
				throw new IllegalArgumentException("Target index is not in tail");

			return rangeFrom(startIndex + 1, endIndex + 1);
		}
		int index = arrayExpression.getStartIndex();
		if (index >= 0)
			throw new UnsupportedOperationException("Head indices are not supported yet");
		index += this.tailSize;
		if (index < 0)
			throw new IllegalArgumentException("Target index is not in tail");

		return IntSets.singleton(index + 1);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#jsonToRecord(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.pact.common.type.PactRecord)
	 */
	@Override
	public PactRecord jsonToRecord(final IJsonNode value, PactRecord target, final EvaluationContext context) {
		IArrayNode others;
		if (target == null || target.getNumFields() != 1) {

			// the first element is the field "others"
			target = new PactRecord(this.getTailSize() + 1);
			others = new ArrayNode();
			target.setField(0, SopremoUtil.wrap(others));
		} else {
			// clear the others field if target was already used
			others = (IArrayNode) SopremoUtil.unwrap(target.getField(0, JsonNodeWrapper.class));
			others.clear();
		}

		IJsonNode arrayElement;
		final int arraySize = ((IArrayNode) value).size();

		// fill the last tailSize elements of the arraynode into the record
		for (int i = 1; i <= this.getTailSize(); i++) {
			arrayElement = ((IArrayNode) value).get(arraySize - i);
			if (!arrayElement.isMissing())
				target.setField(this.getTailSize() - i + 1, SopremoUtil.wrap(arrayElement));
			else
				target.setNull(this.getTailSize() - i + 1);
		}

		if (this.getTailSize() < arraySize)
			// fill the remaining elements of the array into the leading others field
			for (int i = 0; i < ((IArrayNode) value).size() - this.getTailSize(); i++)
				others.add(((IArrayNode) value).get(i));

		// if (this.getHeadTailSize() < ((IArrayNode) value).size()) {
		// // there are still remaining elements in the array we insert them into the others field
		// for (int i = this.getHeadSize(); i < ((IArrayNode) value).size() - this.getTailSize(); i++) {
		// others.add(((IArrayNode) value).get(i));
		// }
		// // fill the rest into tail
		// for (int i = ((IArrayNode) value).size() - this.getTailSize(); i < ((IArrayNode) value).size(); i++) {
		// arrayElement = ((IArrayNode) value).get(i);
		// if (!arrayElement.isMissing()) {
		// target.setField(i - others.size() - 1, SopremoUtil.wrap(arrayElement));
		// }
		// /*
		// * should not happen
		// * else { /
		// * }
		// */
		// }
		//
		// } else { // tail would possibly not get filled entirely, so we don't need to fill the others field
		// for (int i = this.getHeadSize(); i < ((IArrayNode) value).size(); i++) {
		// arrayElement = ((IArrayNode) value).get(i);
		// if (!arrayElement.isMissing()) {
		// target.setField(i + 1, SopremoUtil.wrap(arrayElement));
		// } else { // headSize < size(incoming array) < headSize+ tailSize
		// target.setNull(i + 1);
		// }
		// }
		// }

		return target;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.serialization.Schema#recordToJson(eu.stratosphere.pact.common.type.PactRecord,
	 * eu.stratosphere.sopremo.type.IJsonNode)
	 */
	@Override
	public IJsonNode recordToJson(final PactRecord record, IJsonNode target) {
		if (this.getTailSize() + 1 != record.getNumFields())
			throw new IllegalStateException("Schema does not match to record!");
		if (target == null)
			target = new ArrayNode();
		else
			((IArrayNode) target).clear();
		JsonNodeWrapper recordElement;
		// insert all elements from others
		((IArrayNode) target).addAll((IArrayNode) SopremoUtil.unwrap(record.getField(0,
			JsonNodeWrapper.class)));

		// insert tail of record
		for (int i = 1; i <= this.getTailSize(); i++) {
			recordElement = record.getField(i, JsonNodeWrapper.class);
			if (recordElement != null)
				((IArrayNode) target).add(SopremoUtil.unwrap(recordElement));
		}
		return target;

	}

}
