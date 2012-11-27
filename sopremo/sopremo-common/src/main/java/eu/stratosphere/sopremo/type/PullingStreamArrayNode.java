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

import java.util.Collections;
import java.util.Iterator;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.util.ConversionIterator;

/**
 * @author rico
 * @author Arvid Heise
 */
public class PullingStreamArrayNode extends StreamArrayNode {
	/**
	 * 
	 */
	private static final long serialVersionUID = -3675920432791464317L;

	private Iterator<IJsonNode> source;

	private CachingExpression<IJsonNode> expression = CachingExpression.ofAny(null);

	private EvaluationContext context;

	@SuppressWarnings("unchecked")
	private Iterator<IJsonNode> iterator = Collections.EMPTY_SET.iterator();

	/**
	 * Initializes PullingStreamArrayNode.
	 */
	public PullingStreamArrayNode() {
	}

	@Override
	public IJsonNode getFirst() {
		return this.iterator.next();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.type.IArrayNode#isEmpty()
	 */
	@Override
	public boolean isEmpty() {
		return !this.iterator.hasNext();
	}

	public void setSource(IStreamArrayNode node) {
		this.source = node.iterator();
		this.iterator = new ConversionIterator<IJsonNode, IJsonNode>(this.source) {
			@Override
			protected IJsonNode convert(IJsonNode inputObject) {
				return PullingStreamArrayNode.this.expression.evaluate(inputObject, inputObject, PullingStreamArrayNode.this.context);
			}
		};
	}

	@Override
	public Iterator<IJsonNode> iterator() {
		return this.iterator;
	}

	public void setExpressionAndContext(EvaluationExpression expression, EvaluationContext context) {
		this.expression.setInnerExpression(expression);
		this.context = context;
	}
}