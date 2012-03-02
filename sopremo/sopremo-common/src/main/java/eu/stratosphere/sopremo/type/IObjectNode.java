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
package eu.stratosphere.sopremo.type;

import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;

/**
 * @author Michael Hopstock
 * @author Tommy Neubert
 *
 */
public interface IObjectNode extends IJsonNode, Iterable<Entry<String, IJsonNode>>{

	/**
	 * Binds the given {@link IJsonNode} to the given String within this node. Overwrites the value if this String is already binded.
	 * @param fieldName
	 *  the String where the value should be binded to
	 * @param value
	 * 	the {@link IJsonNode} which should be binded
	 * @return this node
	 */
	public abstract IObjectNode put(final String fieldName, final IJsonNode value);

	/**
	 * Returns the {@link IJsonNode} which is binded to the given String or {@link MissingNode} if no binding is found.
	 * @param fieldName
	 * 	the String where the binded {@link IJsonNode} should be returned for
	 * @return the binded node
	 */
	public abstract IJsonNode get(final String fieldName);

	/**
	 * Removes the binding for the given String
	 * @param fieldName
	 *  the String where the binding should be removed for
	 * @return the {@link IJsonNode} which was binded to this String
	 */
	public abstract IJsonNode remove(final String fieldName);

	/**
	 * Removes any binding from this node.
	 * @return this node
	 */
	public abstract IObjectNode removeAll();

	/**
	 * Returns a Set of all bindings contained in this node.
	 * @return the set of bindings
	 */
	public abstract Set<Entry<String, IJsonNode>> getEntries();

	/**
	 * Creates bindings in this node for all bindings within the given {@link IObjectNode}. Already existing bindings will be overwritten.
	 * @param jsonNode
	 * 	the {@link IObjectNode} which contains all bindings which should be added
	 * @return this node
	 */
	public abstract IObjectNode putAll(final IObjectNode jsonNode);

	/**
	 * Returns an Iterator over all Strings which have existing bindings within this node.
	 * @return iterator over all Strings
	 */
	public abstract Iterator<String> getFieldNames();

	/**
	 * Returns an Iterator over all Bindings within this node.
	 * 
	 * @return iterator over all bindings
	 */
	public abstract Iterator<Entry<String, IJsonNode>> iterator();
	
	/**
	 * Returns the number of bindings contained in this node.
	 * @return number of bindings
	 */
	public abstract int size();

}