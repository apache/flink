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

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.function.MacroBase;
import eu.stratosphere.sopremo.function.SimpleMacro;

/**
 * @author Arvid Heise
 */
public class DefaultRuleFactory extends AbstractRuleFactory {
	private ExpressionRewriter rewriter = new ExpressionRewriter();

	public DefaultRuleFactory(Class<?> ruleType) {
		super(ruleType);
		
		addRewriteRule(new PathExpression(ExpressionRewriter.ANY), new SimpleMacro<PathExpression>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 2289613753252059804L;

			@Override
			public EvaluationExpression call(PathExpression inputExpr, EvaluationContext context) {
				return inputExpr.getFragment(0);
			}
		}); 
	}

	/**
	 * Initializes DefaultRuleFactory.
	 */
	public DefaultRuleFactory() {
		this(null);
	}

	@Override
	protected EvaluationExpression transform(EvaluationExpression expression, RuleContext context) {
		return this.rewriter.rewrite(expression, context);
	}

	public void addRewriteRule(EvaluationExpression expression, MacroBase resolveExpression) {
		this.rewriter.addRewriteRule(expression, resolveExpression);
	}

	public void addRewriteRule(EvaluationExpression expression, EvaluationExpression resolveExpression) {
		this.rewriter.addRewriteRule(expression, resolveExpression);
	}

}
