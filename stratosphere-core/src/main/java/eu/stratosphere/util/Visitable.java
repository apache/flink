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

package eu.stratosphere.util;

/**
 * This interface marks types as visitable during a traversal. The central method <i>accept(...)</i> contains the logic
 * about how to invoke the supplied {@link Visitor} on the visitable object, and how to traverse further.
 * <p>
 * This concept makes it easy to implement for example a depth-first traversal of a tree or DAG with different types of
 * logic during the traversal. The <i>accept(...)</i> method calls the visitor and then send the visitor to its children
 * (or predecessors). Using different types of visitors, different operations can be performed during the traversal, while
 * writing the actual traversal code only once.
 *
 * @see Visitor
 */
public interface Visitable<T extends Visitable<T>> {
	
	/**
	 * Contains the logic to invoke the visitor and continue the traversal.
	 * Typically invokes the pre-visit method of the visitor, then sends the visitor to the children (or predecessors)
	 * and then invokes the post-visit method.
	 * <p>
	 * A typical code example is the following:
	 * <code>
	 * public void accept(Visitor<Contract> visitor) {
	 *     boolean descend = visitor.preVisit(this);	
	 *     if (descend) {
	 *         if (this.input != null) {
	 *             this.input.accept(visitor);
	 *         }
	 *         visitor.postVisit(this);
	 *     }
	 * }
	 * </code>
	 * 
	 * @param visitor The visitor to be called with this object as the parameter.
	 * 
	 * @see Visitor#preVisit(Visitable)
	 * @see Visitor#postVisit(Visitable)
	 */
	void accept(Visitor<T> visitor);
}
