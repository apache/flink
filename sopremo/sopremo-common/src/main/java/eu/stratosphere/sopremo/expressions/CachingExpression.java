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
package eu.stratosphere.sopremo.expressions;

import eu.stratosphere.pact.common.util.ReflectionUtil;
import eu.stratosphere.sopremo.EvaluationContext;
import eu.stratosphere.sopremo.type.IJsonNode;

/**
 * A wrapping expression which saves the last evaluation result and feeds it back into the wrapped expression to
 * minimize object allocation.
 * 
 * @author Arvid Heise
 */
public abstract class CachingExpression<CacheType extends IJsonNode> extends EvaluationExpression {
	/**
	 * 
	 */
	private static final long serialVersionUID = -4428612687995653881L;

	public static <CacheType extends IJsonNode> CachingExpression<CacheType> of(EvaluationExpression expression,
			Class<? extends CacheType> cacheType) {
		try {
			final CacheType cachedVariable = ReflectionUtil.newInstance(cacheType);
			return new EagerCachingExpression<CacheType>(expression, cachedVariable);
		} catch (Exception e) {
			return ofSubclass(expression, cacheType);
		}

	}

	public static <CacheType extends IJsonNode> CachingExpression<CacheType> ofSubclass(
			EvaluationExpression expression, Class<? extends CacheType> cacheType) {
		return new LazyCachingExpression<CacheType>(expression, cacheType);
	}

	public abstract CacheType evaluate(IJsonNode node, EvaluationContext context);

	protected EvaluationExpression expression;

	public CachingExpression(EvaluationExpression expression) {
		this.expression = expression;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public final CacheType evaluate(IJsonNode node, IJsonNode target, EvaluationContext context) {
		// ignores target, maintains its own target
		return evaluate(node, context);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions
	 * .TransformFunction)
	 */
	@Override
	public EvaluationExpression transformRecursively(TransformFunction function) {
		this.expression = this.expression.transformRecursively(function);
		return function.call(expression);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(StringBuilder builder) {
		this.expression.toString(builder);
	}

	private static class EagerCachingExpression<CacheType extends IJsonNode> extends CachingExpression<CacheType> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7026101939583167279L;

		private final CacheType cachedVariable;

		public EagerCachingExpression(EvaluationExpression expression, CacheType cachedVariable) {
			super(expression);
			this.cachedVariable = cachedVariable;
		}

		@Override
		@SuppressWarnings("unchecked")
		public CacheType evaluate(IJsonNode node, EvaluationContext context) {
			return (CacheType) this.expression.evaluate(node, this.cachedVariable, context);
		}
	}

	private static class LazyCachingExpression<CacheType extends IJsonNode> extends CachingExpression<CacheType> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2084630771920876904L;

		private CacheType cachedVariable;

		public LazyCachingExpression(EvaluationExpression expression,
				@SuppressWarnings("unused") Class<? extends CacheType> cacheType) {
			super(expression);
		}

		@Override
		@SuppressWarnings("unchecked")
		public CacheType evaluate(IJsonNode node, EvaluationContext context) {
			return this.cachedVariable = (CacheType) this.expression.evaluate(node, this.cachedVariable, context);
		}
	}

}
