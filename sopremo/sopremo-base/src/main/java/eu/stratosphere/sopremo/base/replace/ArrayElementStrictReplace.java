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
package eu.stratosphere.sopremo.base.replace;

import eu.stratosphere.sopremo.expressions.CachingExpression;
import eu.stratosphere.sopremo.pact.JsonCollector;
import eu.stratosphere.sopremo.pact.SopremoMatch;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * @author Arvid Heise
 */
public class ArrayElementStrictReplace extends ArrayElementReplaceBase<ArrayElementStrictReplace> {
	private static final long serialVersionUID = -3657489526504410342L;

	public static class Implementation extends SopremoMatch {

		private CachingExpression<IJsonNode> dictionaryValueExtraction;

		private int index = 0;

		@Override
		protected void match(final IJsonNode value1, final IJsonNode value2, final JsonCollector out) {
			((IArrayNode) value1).set(this.index, this.dictionaryValueExtraction.evaluate(value2, this.getContext()));
			out.collect(value1);
		}
	}
}
