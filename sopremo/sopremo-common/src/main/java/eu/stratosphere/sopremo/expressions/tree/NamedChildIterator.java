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

import eu.stratosphere.sopremo.expressions.EvaluationExpression;

/**
 * @author Arvid Heise
 */
public abstract class NamedChildIterator implements ChildIterator {
	private int index = -1;
	
	private final String[] childNames;

	public NamedChildIterator(String... childNames) {
		this.childNames = childNames;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.ListIterator#hasNext()
	 */
	@Override
	public boolean hasNext() {
		return this.index < this.childNames.length - 1;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.ListIterator#next()
	 */
	@Override
	public EvaluationExpression next() {
		return get(++this.index);
	}

	/**
	 * @param i
	 * @return
	 */
	protected abstract EvaluationExpression get(int index);

	/*
	 * (non-Javadoc)
	 * @see java.util.ListIterator#hasPrevious()
	 */
	@Override
	public boolean hasPrevious() {
		return this.index > 0;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.ListIterator#previous()
	 */
	@Override
	public EvaluationExpression previous() {
		return get(--this.index);
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.ListIterator#nextIndex()
	 */
	@Override
	public int nextIndex() {
		return this.index + 1;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.ListIterator#previousIndex()
	 */
	@Override
	public int previousIndex() {
		return this.index - 1;
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.ListIterator#remove()
	 */
	@Override
	public void remove() {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see java.util.ListIterator#set(java.lang.Object)
	 */
	@Override
	public void set(EvaluationExpression childExpression) {
		set(this.index, childExpression);
	}

	protected abstract void set(int index, EvaluationExpression childExpression);

	/*
	 * (non-Javadoc)
	 * @see java.util.ListIterator#add(java.lang.Object)
	 */
	@Override
	public void add(EvaluationExpression childExpression) {
		throw new UnsupportedOperationException();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.tree.ChildIterator#isNamed()
	 */
	@Override
	public boolean canChildrenBeRemoved() {
		return false;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.sopremo.expressions.tree.ChildIterator#getChildName()
	 */
	@Override
	public String getChildName() {
		return this.childNames[this.index];
	}

}
