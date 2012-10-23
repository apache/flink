/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.sopremo.base.replace;

import java.util.Iterator;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.base.Replace;
import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoCoGroup;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IStreamArrayNode;

/**
 * @author Arvid Heise
 */
public class ArrayElementReplaceWithDefaultValue extends ArrayElementReplaceBase<ArrayElementReplaceWithDefaultValue> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -4501510761639457262L;

	private EvaluationExpression defaultExpression = Replace.KEEP_VALUE;

	public void setDefaultExpression(EvaluationExpression defaultExpression) {
		if (defaultExpression == null)
			throw new NullPointerException("defaultExpression must not be null");

		this.defaultExpression = defaultExpression;
	}

	public ArrayElementReplaceWithDefaultValue withDefaultExpression(EvaluationExpression prop) {
		this.setDefaultExpression(prop);
		return this;
	}

	public EvaluationExpression getDefaultExpression() {
		return this.defaultExpression;
	}

	public static class Implementation extends SopremoCoGroup {

		private CachingExpression<IJsonNode> dictionaryValueExtraction, defaultExpression;

		private int index = 0;

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoCoGroup#coGroup(eu.stratosphere.sopremo.type.IArrayNode,
		 * eu.stratosphere.sopremo.type.IArrayNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void coGroup(IStreamArrayNode values1, IStreamArrayNode values2, JsonCollector out) {
			final Iterator<IJsonNode> replaceValueIterator = values2.iterator();
			IJsonNode replaceValue = replaceValueIterator.hasNext() ?
				this.dictionaryValueExtraction.evaluate(replaceValueIterator.next(), this.getContext()) : null;

			final Iterator<IJsonNode> valueIterator = values1.iterator();
			final EvaluationContext context = this.getContext();
			while (valueIterator.hasNext()) {
				final IJsonNode value = valueIterator.next();
				final IJsonNode replacement;
				if (replaceValue != null)
					replacement = replaceValue;
				else
					replacement = this.defaultExpression.evaluate(value, context);
				((IArrayNode) value).set(this.index, replacement);
				out.collect(value);
			}
		}
	}

}
