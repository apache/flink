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

import eu.stratosphere.sopremo.expressions.tree.ChildIterator;

/**
 * Used for {@link EvaluationExpression}s that own sub-expressions.
 * 
 * @author Arvid Heise
 */
public interface ExpressionParent extends Iterable<EvaluationExpression> {
	/* (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public ChildIterator iterator();
//	/**
//	 * Returns all directly contained expressions.
//	 * 
//	 * @return the child expressions
//	 */
//	public List<? extends EvaluationExpression> getChildren();
//
//	/**
//	 * Replaces all directly contained expressions.<br />
//	 * Note unless otherwise stated, the number of elements in this list must exactly match the number of elements
//	 * returned by {@link #getChildren()}.<br />
//	 * In general, this method should only be used after some children returned by {@link #getChildren()} have been
//	 * changed.
//	 * 
//	 * @param children
//	 *        the list of child expressions
//	 */
//	public void setChildren(List<? extends EvaluationExpression> children);
}
