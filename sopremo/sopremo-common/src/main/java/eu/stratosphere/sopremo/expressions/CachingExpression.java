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

import java.util.ArrayList;
import java.util.List;

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

	protected EvaluationExpression innerExpression;

	private final Class<CacheType> cacheType;

	public CachingExpression(final EvaluationExpression expression, final Class<CacheType> cacheType) {
		this.innerExpression = expression;
		this.cacheType = cacheType;
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(final Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (this.getClass() != obj.getClass())
			return false;
		return this.innerExpression.equals(((CachingExpression<CacheType>) obj).innerExpression);
	}

	public abstract CacheType evaluate(IJsonNode node, EvaluationContext context);

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#evaluate(eu.stratosphere.sopremo.type.IJsonNode,
	 * eu.stratosphere.sopremo.type.IJsonNode, eu.stratosphere.sopremo.EvaluationContext)
	 */
	@Override
	public final CacheType evaluate(final IJsonNode node, final IJsonNode target, final EvaluationContext context) {
		// ignores target, maintains its own target
		return this.evaluate(node, context);
	}

	public EvaluationExpression getInnerExpression() {
		return this.innerExpression;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + this.innerExpression.hashCode();
		return result;
	}

	@Override
	public IJsonNode set(final IJsonNode node, final IJsonNode value, final EvaluationContext context) {
		return this.innerExpression.set(node, value, context);
	}

	public void setInnerExpression(final EvaluationExpression expression) {
		if (expression == null)
			throw new NullPointerException("expression must not be null");

		this.innerExpression = expression;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.EvaluationExpression#toString(java.lang.StringBuilder)
	 */
	@Override
	public void toString(final StringBuilder builder) {
		this.innerExpression.toString(builder);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.sopremo.expressions.EvaluationExpression#transformRecursively(eu.stratosphere.sopremo.expressions
	 * .TransformFunction)
	 */
	@Override
	public EvaluationExpression transformRecursively(final TransformFunction function) {
		this.innerExpression = this.innerExpression.transformRecursively(function);
		return function.call(this);
	}

	/**
	 * Creates an list of expressions that cache the resulting value with any type of the given expression. If an
	 * expression already caches values, it is directly returned.
	 * 
	 * @param expressions
	 *        the expressions to wrap
	 * @return a list of caching expressions
	 */
	public static List<CachingExpression<IJsonNode>> listOfAny(final List<? extends EvaluationExpression> expressions) {
		final ArrayList<CachingExpression<IJsonNode>> list = new ArrayList<CachingExpression<IJsonNode>>();
		for (final EvaluationExpression evaluationExpression : expressions)
			list.add(ofAny(evaluationExpression));
		return list;
	}

	/**
	 * Creates an expression that caches the resulting value with the specified type of the given expression. If the
	 * given expression already caches values of the same type, it is directly returned.
	 * 
	 * @param expression
	 *        the expression to wrap
	 * @param cacheType
	 *        the expected type of the expression
	 * @return a caching expression
	 */
	@SuppressWarnings("unchecked")
	public static <CacheType extends IJsonNode> CachingExpression<CacheType> of(EvaluationExpression expression,
			final Class<CacheType> cacheType) {
		try {
			if (expression instanceof CachingExpression) {
				if (expression instanceof EagerCachingExpression
					&& ((CachingExpression<?>) expression).cacheType == cacheType)
					return (CachingExpression<CacheType>) expression;
				expression = ((CachingExpression<?>) expression).innerExpression;
			}
			final CacheType cachedVariable = ReflectionUtil.newInstance(cacheType);
			return new EagerCachingExpression<CacheType>(expression, cacheType, cachedVariable);
		} catch (final Exception e) {
			return ofSubclass(expression, cacheType);
		}
	}

	/**
	 * Creates an expression that caches the resulting value with any type of the given expression. If the
	 * given expression already caches values, it is directly returned.
	 * 
	 * @param expression
	 *        the expression to wrap
	 * @return a caching expression
	 */
	public static CachingExpression<IJsonNode> ofAny(final EvaluationExpression expression) {
		return ofSubclass(expression, IJsonNode.class);
	}

	/**
	 * Creates an expression that caches the resulting value with the specified type or a subclass thereof of the given
	 * expression. If the given expression already caches values of the same types, it is directly returned.
	 * 
	 * @param expression
	 *        the expression to wrap
	 * @param cacheType
	 *        the expected type of the expression
	 * @return a caching expression
	 */
	@SuppressWarnings("unchecked")
	public static <CacheType extends IJsonNode> CachingExpression<CacheType> ofSubclass(
			EvaluationExpression expression, final Class<CacheType> cacheType) {
		if (expression instanceof CachingExpression) {
			if (expression instanceof LazyCachingExpression
				&& ((CachingExpression<?>) expression).cacheType == cacheType)
				return (CachingExpression<CacheType>) expression;
			expression = ((CachingExpression<?>) expression).innerExpression;
		}
		return new LazyCachingExpression<CacheType>(expression, cacheType);
	}

	private static class EagerCachingExpression<CacheType extends IJsonNode> extends CachingExpression<CacheType> {
		/**
		 * 
		 */
		private static final long serialVersionUID = 7026101939583167279L;

		private transient final CacheType cachedVariable;

		public EagerCachingExpression(final EvaluationExpression expression, final Class<CacheType> cacheType,
				final CacheType cachedVariable) {
			super(expression, cacheType);
			this.cachedVariable = cachedVariable;
		}

		@Override
		@SuppressWarnings("unchecked")
		public CacheType evaluate(final IJsonNode node, final EvaluationContext context) {
			return (CacheType) this.innerExpression.evaluate(node, this.cachedVariable, context);
		}
	}

	private static class LazyCachingExpression<CacheType extends IJsonNode> extends CachingExpression<CacheType> {
		/**
		 * 
		 */
		private static final long serialVersionUID = -2084630771920876904L;

		private transient CacheType cachedVariable;

		public LazyCachingExpression(final EvaluationExpression expression, final Class<CacheType> cacheType) {
			super(expression, cacheType);
		}

		@Override
		@SuppressWarnings("unchecked")
		public CacheType evaluate(final IJsonNode node, final EvaluationContext context) {
			return this.cachedVariable = (CacheType) this.innerExpression.evaluate(node, this.cachedVariable, context);
		}
	}

}
