/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.util.reflect;

/**
 * A general purpose interface for the visitor pattern in tree strucures.
 * 
 * @author Arvid Heise
 */
public interface Visitor<T> {
	/**
	 * The callback with the current node and its distance to the (relative) root.<br>
	 * If this callback returns <code>false</code>, the visit of the tree is terminated.
	 * 
	 * @param node
	 *        the node
	 * @param distance
	 *        the distance of the node
	 * @return false, if the tree should not be visited anymore
	 */
	boolean visited(T node, int distance);

}
