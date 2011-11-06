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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.Operator;
import eu.stratosphere.sopremo.expressions.ContainerExpression;
import eu.stratosphere.sopremo.expressions.EvaluationExpression;
import eu.stratosphere.sopremo.expressions.PathExpression;
import eu.stratosphere.sopremo.expressions.UnevaluableExpression;
import eu.stratosphere.sopremo.function.MacroBase;
import eu.stratosphere.sopremo.function.ReplacingMacro;
import eu.stratosphere.util.reflect.DynamicProperty;
import eu.stratosphere.util.reflect.ReflectUtil;

/**
 * @author Arvid Heise
 */
public class ExpressionRewriter {
	public static final EvaluationExpression ANY = new UnevaluableExpression("<any>") {
		/**
		 * 
		 */
		private static final long serialVersionUID = 6219948257307870742L;

		@Override
		public boolean equals(Object obj) {
			return obj instanceof EvaluationExpression;
		};
	};

	public static final class ExpressionType extends UnevaluableExpression {
		/**
		 * 
		 */
		private static final long serialVersionUID = 3000177685098016306L;

		private Class<? extends EvaluationExpression> type;

		public ExpressionType(Class<? extends EvaluationExpression> type) {
			super("type " + type.getSimpleName());
			this.type = type;
		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.expressions.UnevaluableExpression#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {
			return this.type.isInstance(obj);
		}
	}

	private List<AbstractMap.SimpleEntry<EvaluationExpression, MacroBase>> rewriteRules = new ArrayList<AbstractMap.SimpleEntry<EvaluationExpression, MacroBase>>();

	public void addRewriteRule(EvaluationExpression expression, MacroBase resolveExpression) {
		this.rewriteRules.add(new AbstractMap.SimpleEntry<EvaluationExpression, MacroBase>(expression,
			resolveExpression));
	}

	public void addRewriteRule(EvaluationExpression expression, EvaluationExpression resolveExpression) {
		addRewriteRule(expression, new ReplacingMacro("", resolveExpression));
	}

	/**
	 * @param rule
	 * @param expression
	 * @return
	 */
	private boolean matches(EvaluationExpression rule, EvaluationExpression expression) {
		return rule.equals(expression);
	}

	/**
	 * @param expression
	 * @return
	 */
	private MacroBase matchRules(EvaluationExpression expression) {
		for (AbstractMap.SimpleEntry<EvaluationExpression, MacroBase> rule : this.rewriteRules)
			if (this.matches(rule.getKey(), expression))
				return rule.getValue();
		return null;
	}

	public EvaluationExpression rewrite(EvaluationExpression expression, Operator<?> operator, PathExpression context) {
		return new Rewriter(operator, context).rewrite(expression);
	}

	/**
	 * @author Arvid Heise
	 */
	private final class Rewriter {
		private Map<EvaluationExpression, EvaluationExpression> rewritten = new IdentityHashMap<EvaluationExpression, EvaluationExpression>();

		private RewriteContext context = new RewriteContext();

		public Rewriter(Operator<?> operator, PathExpression rewritePath) {
			context.pushOperator(operator);
			context.setRewritePath(rewritePath);
		}

		private EvaluationExpression process(EvaluationExpression expression) {
			if (this.rewritten.containsKey(expression))
				return this.rewritten.get(expression);

			MacroBase macro = ExpressionRewriter.this.matchRules(expression);
			if (macro != null) {
				EvaluationExpression rewrittenExpression = macro.call(new EvaluationExpression[] { expression },
					this.context);
				this.rewritten.put(expression, rewrittenExpression);
				return rewrittenExpression;
			}

			return expression;
		}

		/**
		 * @param expression
		 * @return
		 */
		public EvaluationExpression rewrite(EvaluationExpression expression) {
			EvaluationExpression rewritten = process(expression);
			if(rewritten == expression) {
			if (expression instanceof ContainerExpression) {
				List<EvaluationExpression> children = new ArrayList<EvaluationExpression>(
					((ContainerExpression) expression).getChildren());
				for (int index = 0; index < children.size(); index++)
					children.set(index, this.rewrite(children.get(index)));
				((ContainerExpression) expression).setChildren(children);
			}
			
			for(DynamicProperty<EvaluationExpression> property : ReflectUtil.getDynamicClass(expression.getClass()).getProperties(EvaluationExpression.class)) {
				EvaluationExpression propertyValue = property.get(expression);
				EvaluationExpression newValue = rewrite(propertyValue);
				if(propertyValue != newValue)
					property.set(expression, newValue);
			}
			}
			return rewritten;
		}
	}
}
