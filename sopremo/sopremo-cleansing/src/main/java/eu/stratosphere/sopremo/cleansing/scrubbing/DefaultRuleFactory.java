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

import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.function.MacroBase;

/**
 * @author Arvid Heise
 */
public class DefaultRuleFactory extends AbstractRuleFactory {
	private ExpressionRewriter rewriter = new ExpressionRewriter();

	public DefaultRuleFactory(Class<?> ruleType) {
		super(ruleType);
	}

	/**
	 * Initializes DefaultRuleFactory.
	 */
	public DefaultRuleFactory() {
		this(null);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.cleansing.scrubbing.AbstractRuleFactory#transform(eu.stratosphere.sopremo.expressions
	 * .EvaluationExpression, eu.stratosphere.sopremo.Operator, java.util.Deque)
	 */
	@Override
	protected EvaluationExpression transform(EvaluationExpression expression, Operator<?> operator,
			PathExpression contextPath) {
		return this.rewriter.rewrite(expression, operator, contextPath);
	}

	public void addRewriteRule(EvaluationExpression expression, MacroBase resolveExpression) {
		this.rewriter.addRewriteRule(expression, resolveExpression);
	}

	public void addRewriteRule(EvaluationExpression expression, EvaluationExpression resolveExpression) {
		this.rewriter.addRewriteRule(expression, resolveExpression);
	}

}
