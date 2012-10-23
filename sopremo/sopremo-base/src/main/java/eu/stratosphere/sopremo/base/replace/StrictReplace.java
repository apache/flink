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

import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.operator.InputCardinality;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * Replaces values in the first source by values in the dictionary given in the second source.
 * 
 * @author Arvid Heise
 */
@InputCardinality(min = 2, max = 2)
public class StrictReplace extends ReplaceBase<StrictReplace> {
	private static final long serialVersionUID = 7334161941683036846L;

	public static class Implementation extends SopremoMatch {
		private EvaluationExpression replaceExpression;

		private CachingExpression<IJsonNode> dictionaryValueExtraction;

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.pact.SopremoMatch#match(eu.stratosphere.sopremo.type.IJsonNode,
		 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.pact.JsonCollector)
		 */
		@Override
		protected void match(IJsonNode value1, IJsonNode value2, JsonCollector out) {
			out.collect(this.replaceExpression.set(value1,
				this.dictionaryValueExtraction.evaluate(value2, this.getContext()),
				this.getContext()));
		}
	}
}