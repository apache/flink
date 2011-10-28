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
package eu.stratosphere.sopremo.cleansing.scrubbing;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.sopremo.expressions.ContainerExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.function.MacroBase;
import eu.stratosphere.util.dag.Navigator;
import eu.stratosphere.util.dag.OneTimeTraverser;

/**
 * @author Arvid Heise
 */
public class ExpressionRewriter {
	private Map<EvaluationExpression, MacroBase> rewriteRules = new HashMap<EvaluationExpression, MacroBase>();

	private Navigator<EvaluationExpression> ExpressionNavigator = new Navigator<EvaluationExpression>() {
		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.util.dag.Navigator#getConnectedNodes(java.lang.Object)
		 */
		@SuppressWarnings("unchecked")
		@Override
		public Iterable<? extends EvaluationExpression> getConnectedNodes(EvaluationExpression node) {
			if (node instanceof ContainerExpression)
				return node;
			return Collections.EMPTY_LIST;
		}
	};
	

	public EvaluationExpression rewrite(EvaluationExpression expression) {
		OneTimeTraverser.INSTANCE.traverse(expression, ExpressionNavigator, null);
		return expression;
	}
}
