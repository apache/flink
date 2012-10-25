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
package eu.stratosphere.sopremo.expressions.tree;

import java.util.ListIterator;

import eu.stratosphere.sopremo.expressions.EvaluationExpression;

/**
 * @author Arvid Heise
 */
public abstract class GenericListChildIterator<E extends EvaluationExpression> implements ChildIterator {
	private final ListIterator<E> expressionIterator;

	public GenericListChildIterator(ListIterator<E> expressionIterator) {
		this.expressionIterator = expressionIterator;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.tree.ChildIterator#getChildName()
	 */
	@Override
	public String getChildName() {
		return null;
	}

	@Override
	public boolean hasNext() {
		return this.expressionIterator.hasNext();
	}

	@Override
	public boolean hasPrevious() {
		return this.expressionIterator.hasPrevious();
	}
	
	/* (non-Javadoc)
	 * @see java.util.ListIterator#add(java.lang.Object)
	 */
	@Override
	public void add(EvaluationExpression e) {
		this.expressionIterator.add(convert(e));
	}
	
	protected abstract E convert(EvaluationExpression childExpression);

	/* (non-Javadoc)
	 * @see java.util.ListIterator#set(java.lang.Object)
	 */
	@Override
	public void set(EvaluationExpression e) {
		this.expressionIterator.set(convert(e));
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.tree.ChildIterator#isNamed()
	 */
	@Override
	public boolean canChildrenBeRemoved() {
		return true;
	}

	@Override
	public EvaluationExpression next() {
		return this.expressionIterator.next();
	}

	@Override
	public int nextIndex() {
		return this.expressionIterator.nextIndex();
	}

	@Override
	public EvaluationExpression previous() {
		return this.expressionIterator.previous();
	}

	@Override
	public int previousIndex() {
		return this.expressionIterator.previousIndex();
	}

	@Override
	public void remove() {
		this.expressionIterator.remove();
	}

	/**
	 * Returns the expressionIterator.
	 * 
	 * @return the expressionIterator
	 */
	protected ListIterator<E> getExpressionIterator() {
		return this.expressionIterator;
	}

}
